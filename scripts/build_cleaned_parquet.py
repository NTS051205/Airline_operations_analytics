"""Build and validate the Phase 2 cleaned BTS Parquet dataset."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession

from airline_analytics.cleaning import CLEANED_COLUMNS, clean_flights
from airline_analytics.ingestion import load_raw_csvs, validate_input_files
from airline_analytics.schema import EXPECTED_FILE_METADATA
from airline_analytics.validation import (
    collect_arr_del15_violation_samples,
    collect_cleaned_summary,
    collect_raw_summary,
    collect_transformation_issues,
    compare_cleaned_schema,
)


OUTPUT_PARTITIONS = 3


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build validated Phase 2 cleaned Parquet data."
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=Path("data/raw"),
        help="Directory containing the three verified BTS CSV files.",
    )
    parser.add_argument(
        "--phase1-profile",
        type=Path,
        default=Path("artifacts/phase1/profile_summary.json"),
        help="Verified Phase 1 JSON used as the count baseline.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/processed/flights_cleaned"),
        help="New Parquet directory. The script refuses an existing path.",
    )
    parser.add_argument(
        "--validation-output",
        type=Path,
        default=Path("artifacts/phase2/validation_summary.json"),
        help="New JSON report path. The script refuses to overwrite it.",
    )
    return parser.parse_args()


def create_spark_session() -> SparkSession:
    """Create a conservative Spark session for the 12 GB local machine."""
    spark = (
        SparkSession.builder.appName("airline-phase2-cleaning")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.sql.csv.parser.columnPruning.enabled", "false")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def _month_count_map(items: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in items:
        year = item.get("year")
        month = item.get("month")
        key = (
            f"{int(year):04d}-{int(month):02d}"
            if year is not None and month is not None
            else "null_or_invalid_partition"
        )
        counts[key] = counts.get(key, 0) + int(item["row_count"])
    return dict(sorted(counts.items()))


def load_phase1_baseline(path: Path) -> dict[str, Any]:
    """Load verified Phase 1 counts without hardcoding them in Phase 2 code."""
    if not path.is_file():
        raise FileNotFoundError(f"Phase 1 profile not found: {path}")
    with path.open("r", encoding="utf-8") as profile_file:
        profile = json.load(profile_file)

    file_counts = profile["row_counts"]["by_file"]
    status = profile["flight_status"]
    invalid_count = int(status.get("invalid_both_flags", 0)) + int(
        status.get("invalid_or_missing_flags", 0)
    )
    return {
        "profile_path": path.as_posix(),
        "total_rows": int(profile["row_counts"]["total"]),
        "by_month": [
            {
                "year": int(item["source_year"]),
                "month": int(item["source_month"]),
                "row_count": int(item["row_count"]),
            }
            for item in file_counts
        ],
        "flight_status": {
            "completed": int(status["completed"]),
            "cancelled": int(status["cancelled"]),
            "diverted": int(status["diverted"]),
            "invalid": invalid_count,
        },
    }


def _check(name: str, passed: bool, details: Any) -> dict[str, Any]:
    return {"name": name, "passed": bool(passed), "details": details}


def build_pre_write_validation(
    raw_df: DataFrame,
    cleaned_df: DataFrame,
    phase1_baseline: dict[str, Any],
) -> dict[str, Any]:
    """Run all mandatory checks before any Parquet write occurs."""
    raw = collect_raw_summary(raw_df)
    cleaned = collect_cleaned_summary(cleaned_df)
    transformations = collect_transformation_issues(raw_df)
    schema = compare_cleaned_schema(cleaned_df)

    raw_months = _month_count_map(raw["by_month"])
    cleaned_months = _month_count_map(cleaned["by_month"])
    baseline_months = _month_count_map(phase1_baseline["by_month"])
    output_binary_invalid = sum(cleaned["binary_invalid_counts"].values())
    output_hhmm_invalid = sum(cleaned["hhmm_invalid_counts"].values())

    checks = [
        _check("raw_dataset_is_not_empty", raw["total_rows"] > 0, raw["total_rows"]),
        _check(
            "raw_count_equals_cleaned_count",
            raw["total_rows"] == cleaned["total_rows"],
            {"raw": raw["total_rows"], "cleaned": cleaned["total_rows"]},
        ),
        _check(
            "month_counts_unchanged",
            raw_months == cleaned_months,
            {"raw": raw_months, "cleaned": cleaned_months},
        ),
        _check(
            "status_counts_unchanged",
            raw["flight_status"] == cleaned["flight_status"],
            {
                "raw": raw["flight_status"],
                "cleaned": cleaned["flight_status"],
            },
        ),
        _check(
            "status_reconciles_to_cleaned_count",
            cleaned["status_reconciles"],
            {
                "status_sum": cleaned["status_sum"],
                "cleaned_rows": cleaned["total_rows"],
            },
        ),
        _check(
            "no_invalid_flight_status",
            cleaned["flight_status"]["invalid"] == 0,
            cleaned["flight_status"]["invalid"],
        ),
        _check("cleaned_schema_matches_contract", schema["pass"], schema),
        _check(
            "no_unexpected_cast_or_hhmm_issues",
            transformations["pass"],
            transformations,
        ),
        _check(
            "output_binary_domains_are_valid",
            output_binary_invalid == 0,
            cleaned["binary_invalid_counts"],
        ),
        _check(
            "output_hhmm_values_are_valid_strings",
            output_hhmm_invalid == 0,
            cleaned["hhmm_invalid_counts"],
        ),
        _check(
            "source_file_lineage_is_present",
            cleaned["source_file_null_count"] == 0,
            cleaned["source_file_null_count"],
        ),
        _check(
            "arr_del15_row_level_rules",
            cleaned["arr_del15_checks"]["pass"],
            cleaned["arr_del15_checks"],
        ),
        _check(
            "raw_count_matches_phase1_baseline",
            raw["total_rows"] == phase1_baseline["total_rows"],
            {
                "raw": raw["total_rows"],
                "phase1": phase1_baseline["total_rows"],
            },
        ),
        _check(
            "raw_months_match_phase1_baseline",
            raw_months == baseline_months,
            {"raw": raw_months, "phase1": baseline_months},
        ),
        _check(
            "raw_status_matches_phase1_baseline",
            raw["flight_status"] == phase1_baseline["flight_status"],
            {
                "raw": raw["flight_status"],
                "phase1": phase1_baseline["flight_status"],
            },
        ),
    ]

    violation_samples: list[dict[str, Any]] = []
    if not cleaned["arr_del15_checks"]["pass"]:
        violation_samples = collect_arr_del15_violation_samples(cleaned_df)

    passed = all(check["passed"] for check in checks)
    return {
        "status": "PASS" if passed else "FAIL",
        "raw": raw,
        "cleaned": cleaned,
        "phase1_baseline": phase1_baseline,
        "schema": schema,
        "cast_and_null_checks": transformations,
        "arr_del15_violation_samples": violation_samples,
        "checks": checks,
        "failed_checks": [
            check["name"] for check in checks if not check["passed"]
        ],
    }


def build_read_back_validation(
    pre_write_cleaned: dict[str, Any],
    read_back_df: DataFrame,
    schema: dict[str, Any],
) -> dict[str, Any]:
    """Compare Parquet read-back metrics with the pre-write DataFrame."""
    read_back = collect_cleaned_summary(read_back_df)
    pre_months = _month_count_map(pre_write_cleaned["by_month"])
    read_back_months = _month_count_map(read_back["by_month"])
    read_back_binary_invalid = sum(read_back["binary_invalid_counts"].values())
    read_back_hhmm_invalid = sum(read_back["hhmm_invalid_counts"].values())
    checks = [
        _check(
            "read_back_count_matches_pre_write",
            read_back["total_rows"] == pre_write_cleaned["total_rows"],
            {
                "pre_write": pre_write_cleaned["total_rows"],
                "read_back": read_back["total_rows"],
            },
        ),
        _check(
            "read_back_months_match_pre_write",
            read_back_months == pre_months,
            {"pre_write": pre_months, "read_back": read_back_months},
        ),
        _check(
            "read_back_status_matches_pre_write",
            read_back["flight_status"]
            == pre_write_cleaned["flight_status"],
            {
                "pre_write": pre_write_cleaned["flight_status"],
                "read_back": read_back["flight_status"],
            },
        ),
        _check("read_back_schema_matches_contract", schema["pass"], schema),
        _check(
            "read_back_status_reconciles",
            read_back["status_reconciles"],
            {
                "status_sum": read_back["status_sum"],
                "rows": read_back["total_rows"],
            },
        ),
        _check(
            "read_back_arr_del15_rules",
            read_back["arr_del15_checks"]["pass"],
            read_back["arr_del15_checks"],
        ),
        _check(
            "read_back_binary_domains_are_valid",
            read_back_binary_invalid == 0,
            read_back["binary_invalid_counts"],
        ),
        _check(
            "read_back_hhmm_values_are_valid_strings",
            read_back_hhmm_invalid == 0,
            read_back["hhmm_invalid_counts"],
        ),
        _check(
            "read_back_lineage_present",
            read_back["source_file_null_count"] == 0,
            read_back["source_file_null_count"],
        ),
    ]
    passed = all(check["passed"] for check in checks)
    return {
        "status": "PASS" if passed else "FAIL",
        "summary": read_back,
        "schema": schema,
        "checks": checks,
        "failed_checks": [
            check["name"] for check in checks if not check["passed"]
        ],
    }


def write_cleaned_parquet(cleaned_df: DataFrame, output_dir: Path) -> None:
    """Write three-month MVP data with one hash partition per month key."""
    if output_dir.exists():
        raise FileExistsError(f"Output directory already exists: {output_dir}")
    (
        cleaned_df.repartition(OUTPUT_PARTITIONS, "YEAR", "MONTH")
        .write.mode("errorifexists")
        .option("compression", "snappy")
        .partitionBy("YEAR", "MONTH")
        .parquet(output_dir.resolve().as_posix())
    )


def _initial_report(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "phase": 2,
        "scope": "BTS Reporting Carrier On-Time Performance, 2025-01 to 2025-03",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "overall_status": "RUNNING",
        "failure_stage": None,
        "input": {
            "raw_directory": args.input_dir.as_posix(),
            "phase1_profile": args.phase1_profile.as_posix(),
            "validated_files": None,
        },
        "output": {
            "path": args.output_dir.as_posix(),
            "format": "parquet",
            "compression": "snappy",
            "partition_columns": ["YEAR", "MONTH"],
            "shuffle_partitions_for_write": OUTPUT_PARTITIONS,
        },
        "pre_write": {"status": "NOT_RUN"},
        "write": {"status": "NOT_RUN"},
        "read_back": {"status": "NOT_RUN"},
        "errors": [],
        "limitations": [
            "PASS covers only the implemented Phase 2 rules.",
            "Conditional source nulls are preserved rather than imputed.",
            "No duplicate removal or outlier capping is performed.",
            "This pipeline does not create analytics, Power BI, or ML outputs.",
        ],
    }


def write_validation_report(report: dict[str, Any], path: Path) -> None:
    """Write one new JSON report without overwriting prior evidence."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x", encoding="utf-8") as report_file:
        json.dump(report, report_file, indent=2, ensure_ascii=False)


def print_summary(report: dict[str, Any], report_path: Path) -> None:
    print("\nPHASE 2 CLEANING SUMMARY")
    print(f"Overall status: {report['overall_status']}")
    if report["pre_write"].get("cleaned"):
        cleaned = report["pre_write"]["cleaned"]
        print(f"Cleaned rows: {cleaned['total_rows']:,}")
        print(f"Flight status: {cleaned['flight_status']}")
    print(f"Parquet write: {report['write']['status']}")
    print(f"Parquet read-back: {report['read_back']['status']}")
    print(f"Validation report: {report_path.resolve()}")


def main() -> None:
    args = parse_args()
    if args.validation_output.exists():
        raise FileExistsError(
            f"Validation report already exists: {args.validation_output}"
        )

    report = _initial_report(args)
    spark: SparkSession | None = None
    raw_df: DataFrame | None = None
    cleaned_df: DataFrame | None = None
    current_stage = "preflight"

    try:
        if args.output_dir.exists():
            raise FileExistsError(
                f"Output directory already exists: {args.output_dir}"
            )

        phase1_baseline = load_phase1_baseline(args.phase1_profile)
        input_paths = [
            args.input_dir / file_name for file_name in EXPECTED_FILE_METADATA
        ]
        report["input"]["validated_files"] = validate_input_files(input_paths)

        current_stage = "pre_write"
        report["pre_write"] = {"status": "RUNNING"}
        spark = create_spark_session()
        raw_df = load_raw_csvs(spark, input_paths).persist(
            StorageLevel.MEMORY_AND_DISK
        )
        cleaned_df = clean_flights(raw_df).persist(StorageLevel.MEMORY_AND_DISK)
        report["pre_write"] = build_pre_write_validation(
            raw_df, cleaned_df, phase1_baseline
        )
        if report["pre_write"]["status"] != "PASS":
            raise RuntimeError(
                "Pre-write validation failed: "
                + ", ".join(report["pre_write"]["failed_checks"])
            )

        raw_df.unpersist()
        raw_df = None

        current_stage = "write"
        report["write"] = {"status": "RUNNING"}
        write_cleaned_parquet(cleaned_df, args.output_dir)
        report["write"] = {"status": "PASS"}

        cleaned_df.unpersist()
        cleaned_df = None

        current_stage = "read_back"
        report["read_back"] = {"status": "RUNNING"}
        loaded_parquet = spark.read.parquet(args.output_dir.resolve().as_posix())
        read_back_schema = compare_cleaned_schema(loaded_parquet)
        if not read_back_schema["pass"]:
            report["read_back"] = {
                "status": "FAIL",
                "schema": read_back_schema,
                "checks": [],
                "failed_checks": ["read_back_schema_matches_contract"],
            }
            raise RuntimeError("Parquet read-back schema validation failed")

        read_back_df = loaded_parquet.select(*CLEANED_COLUMNS)
        report["read_back"] = build_read_back_validation(
            report["pre_write"]["cleaned"],
            read_back_df,
            read_back_schema,
        )
        if report["read_back"]["status"] != "PASS":
            raise RuntimeError(
                "Parquet read-back validation failed: "
                + ", ".join(report["read_back"]["failed_checks"])
            )

        report["overall_status"] = "PASS"
        write_validation_report(report, args.validation_output)
        print_summary(report, args.validation_output)
    except Exception as error:
        report["overall_status"] = "FAIL"
        report["failure_stage"] = current_stage
        stage_report = report.get(current_stage)
        if isinstance(stage_report, dict) and stage_report.get("status") == "RUNNING":
            stage_report["status"] = "FAIL"
        report["errors"].append(
            {"type": type(error).__name__, "message": str(error)}
        )
        if not args.validation_output.exists():
            write_validation_report(report, args.validation_output)
        print_summary(report, args.validation_output)
        raise
    finally:
        if cleaned_df is not None:
            cleaned_df.unpersist()
        if raw_df is not None:
            raw_df.unpersist()
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()
