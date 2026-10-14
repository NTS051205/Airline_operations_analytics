"""Build four Phase 3A Spark SQL outputs and validate their Parquet round trip."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pyspark import StorageLevel
from pyspark.sql import SparkSession

from airline_analytics.analytics_validation import (
    TABLE_GRAINS,
    compare_mvp_table,
    reconcile_baseline,
    reconcile_months,
    require_checks,
    summarize_table,
    validate_input,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SQL_DIR = PROJECT_ROOT / "sql" / "analytics"
BUSINESS_QUERIES = ("005_airline_ranking.sql", "006_airline_mom.sql", "007_high_delay_routes.sql")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", type=Path, default=PROJECT_ROOT / "data/processed/flights_cleaned_2025_reviewed")
    parser.add_argument("--phase2-report", type=Path, default=PROJECT_ROOT / "artifacts/phase2_2025_reviewed/validation_summary.json")
    parser.add_argument("--output-dir", type=Path, default=PROJECT_ROOT / "data/analytics/2025_reviewed")
    parser.add_argument("--validation-output", type=Path, default=PROJECT_ROOT / "artifacts/phase3_2025_reviewed/validation_summary.json")
    parser.add_argument("--mvp-analytics-dir", type=Path, default=PROJECT_ROOT / "data/analytics",
                        help="Read-only parent of the four existing January-March Parquet tables.")
    parser.add_argument("--min-completed-flights", type=int, default=30,
                        help="Adjustable volume heuristic for business queries only.")
    parser.add_argument("--preview-rows", type=int, default=20,
                        help="Console rows per business query, from 1 to 100.")
    args = parser.parse_args()
    if args.min_completed_flights < 1 or not 1 <= args.preview_rows <= 100:
        parser.error("Minimum flights must be positive; preview rows must be 1-100.")
    return args


def check_paths(args: argparse.Namespace) -> None:
    """Reject existing outputs and overlapping input/output locations."""
    # Protect the four MVP tables, not their parent: the new annual directory
    # is a sibling of those tables inside data/analytics/.
    protected = [p.resolve() for p in (
        args.input_dir, args.phase2_report.parent,
        *[args.mvp_analytics_dir / name for name in TABLE_GRAINS],
    )]
    outputs = [args.output_dir.resolve(), args.validation_output.resolve()]
    for index, left in enumerate(outputs):
        for right in protected + outputs[index + 1:]:
            if left.is_relative_to(right) or right.is_relative_to(left):
                raise ValueError(f"Input/report/output paths must not overlap: {left}, {right}")
    if not args.input_dir.is_dir():
        raise FileNotFoundError(f"Cleaned Parquet directory not found: {args.input_dir}")
    if args.output_dir.exists():
        raise FileExistsError(f"Refusing existing analytics directory: {args.output_dir}")
    if args.validation_output.exists():
        raise FileExistsError(f"Refusing existing validation report: {args.validation_output}")


def load_baseline(path: Path, input_dir: Path) -> dict[str, Any]:
    """Read recorded Phase 2 evidence; do not rerun cleaning or hardcode results."""
    content = path.read_bytes()
    evidence = json.loads(content.decode("utf-8-sig"))
    if evidence["phase"] != 2 or evidence["overall_status"] != "PASS" or evidence["errors"]:
        raise ValueError("Phase 2 evidence must record PASS without errors.")
    for stage in ("pre_write", "write", "read_back"):
        if evidence[stage]["status"] != "PASS":
            raise ValueError(f"Phase 2 {stage} is not PASS.")
    if any(evidence["write"].get(name) != "PASS" for name in ("accepted_status", "rejected_status")):
        raise ValueError("Both Phase 2 accepted and rejected writes must be PASS.")
    for stage in ("pre_write", "read_back"):
        checks = evidence[stage]["checks"]
        if not checks or not all(check["passed"] is True for check in checks):
            raise ValueError(f"Phase 2 {stage} contains missing/failed checks.")
        if evidence[stage].get("failed_checks") != []:
            raise ValueError(f"Phase 2 {stage} failed_checks must be empty.")
    if evidence.get("failed_checks", []):
        raise ValueError("Phase 2 contains failed checks.")
    required_checks = {
        "pre_write": ("rejected_matches_approved_diagnostic", "cleaned_has_essential_arrival"),
        "read_back": ("read_back_essential_arrival_present", "read_back_partition_counts_unchanged"),
    }
    for stage, names in required_checks.items():
        passed = {c["name"] for c in evidence[stage]["checks"] if c["passed"] is True}
        if not set(names).issubset(passed):
            raise ValueError(f"Phase 2 {stage} lacks required reviewed-cleaning checks.")
    recorded_path = Path(evidence["output"]["path"])
    if not recorded_path.is_absolute():
        recorded_path = PROJECT_ROOT / recorded_path
    if recorded_path.resolve() != input_dir.resolve():
        raise ValueError("Input Parquet does not match the cleaned output path in Phase 2 evidence.")
    baseline = evidence["read_back"]["summary"]
    cleaned = evidence["pre_write"]["cleaned"]
    for key in ("total_rows", "by_month", "flight_status"):
        if baseline[key] != cleaned[key]:
            raise ValueError(f"Phase 2 pre-write/read-back disagree on {key}.")
    by_month = {
        (r["year"], r["month"]): {
            "YEAR": r["year"], "MONTH": r["month"], "total_flights": r["row_count"],
            "completed_flights": 0, "cancelled_flights": 0, "diverted_flights": 0,
        } for r in baseline["by_month"]
    }
    if len(by_month) != len(baseline["by_month"]):
        raise ValueError("Phase 2 contains duplicate month summaries.")
    partitions = evidence["pre_write"]["partition_counts"]["cleaned"]
    restored_partitions = next(c["details"]["cleaned"] for c in evidence["read_back"]["checks"]
                               if c["name"] == "read_back_partition_counts_unchanged")
    if partitions != restored_partitions:
        raise ValueError("Phase 2 monthly status evidence changed on read-back.")
    for row in partitions:
        period = (row["year"], row["month"])
        status = row["flight_status"]
        if period not in by_month or status not in ("completed", "cancelled", "diverted") or row["count"] < 0:
            raise ValueError("Invalid Phase 2 file/month/status evidence.")
        by_month[period][status + "_flights"] += row["count"]
    statuses = ("completed", "cancelled", "diverted")
    if (sum(r["total_flights"] for r in by_month.values()) != baseline["total_rows"]
            or baseline["flight_status"]["invalid"] != 0
            or any(sum(r[s + "_flights"] for s in statuses) != r["total_flights"]
                   for r in by_month.values())
            or any(sum(r[s + "_flights"] for r in by_month.values()) != baseline["flight_status"][s]
                   for s in statuses)):
        raise ValueError("Phase 2 monthly/status totals do not reconcile internally.")
    return {
        **{key: baseline[key] for key in ("total_rows", "by_month", "flight_status")},
        "status_by_month": [by_month[key] for key in sorted(by_month)],
        "provenance": {"phase2_report": path.resolve().as_posix(),
                       "sha256": hashlib.sha256(content).hexdigest(),
                       "cleaned_path": recorded_path.resolve().as_posix(),
                       "raw_rows": evidence["pre_write"]["raw"]["total_rows"],
                       "rejected_rows": evidence["pre_write"]["rejected"]["total_rows"],
                       "cleaning_policy": evidence["cleaning_policy"]},
    }


def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder.appName("airline-phase3a-analytics")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def initial_report(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "phase": "3A",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "overall_status": "RUNNING",
        "failure_stage": None,
        "input_path": args.input_dir.resolve().as_posix(),
        "phase2_report_path": args.phase2_report.resolve().as_posix(),
        "mvp_reference_paths": {
            name: (args.mvp_analytics_dir / name).resolve().as_posix() for name in TABLE_GRAINS
        },
        "output": {"directory": args.output_dir.resolve().as_posix(),
                   "format": "parquet", "compression": "snappy",
                   "partition_columns": [], "max_data_files_per_table": 1},
        "volume_heuristic": args.min_completed_flights,
        "preflight": {"status": "NOT_RUN"},
        "input": {"status": "NOT_RUN"},
        "pre_write": {"status": "NOT_RUN", "tables": {}},
        "write": {"status": "NOT_RUN", "tables": {}},
        "read_back": {"status": "NOT_RUN", "tables": {}},
        "mvp_regression": {"status": "NOT_RUN", "tables": {}},
        "business_queries": {"status": "NOT_RUN", "queries": {}},
        "errors": [],
        "limitations": [
            "PASS covers these checks, not every possible data-quality issue.",
            "No benchmark, causal analysis, long-term trend or ML evaluation.",
            "Business query previews are not reviewed business conclusions.",
            "Volume threshold is a heuristic, not a statistical confidence guarantee.",
            "Partial outputs are retained on failure; there is no automatic cleanup.",
            "MVP regression uses exact counts/keys and documented DOUBLE tolerance, not rounding.",
            "Input path and baseline checks do not provide a full Parquet content fingerprint.",
        ],
    }


def write_report(report: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x", encoding="utf-8") as handle:
        json.dump(report, handle, indent=2, ensure_ascii=False, allow_nan=False)


def main() -> None:
    args = parse_args()
    # Unsafe/existing output paths fail before creating or replacing evidence.
    check_paths(args)
    report = initial_report(args)
    spark = None
    shared_summary = None
    stage = "preflight"
    try:
        report[stage]["status"] = "RUNNING"
        baseline = load_baseline(args.phase2_report, args.input_dir)
        for name in TABLE_GRAINS:
            reference_path = args.mvp_analytics_dir / name
            if not reference_path.is_dir() or not any(reference_path.glob("*.parquet")):
                raise FileNotFoundError(f"MVP Parquet table not found: {reference_path}")
        report[stage] = {"status": "PASS", "phase2_baseline": baseline}
        spark = create_spark_session()
        report["runtime"] = {"spark_version": spark.version,
                             "master": spark.sparkContext.master,
                             "shuffle_partitions": spark.conf.get("spark.sql.shuffle.partitions")}
        stage = "input"
        report[stage]["status"] = "RUNNING"
        cleaned = spark.read.parquet(args.input_dir.resolve().as_posix())
        cleaned.createOrReplaceTempView("flights_cleaned")
        report[stage] = validate_input(spark, cleaned, baseline)
        require_checks(report[stage], "Cleaned input")

        stage = "pre_write"
        report[stage]["status"] = "RUNNING"
        shared_summary = spark.sql((SQL_DIR / "000_kpi_summary.sql").read_text(encoding="utf-8"))
        shared_summary.persist(StorageLevel.MEMORY_AND_DISK)
        shared_summary.createOrReplaceTempView("kpi_summary")
        tables = {}
        for index, (name, grain) in enumerate(TABLE_GRAINS.items(), start=1):
            report[stage]["tables"][name] = {"status": "RUNNING"}
            query_path = SQL_DIR / f"{index:03d}_{name}.sql"
            df = spark.sql(query_path.read_text(encoding="utf-8"))
            tables[name] = df
            summary = summarize_table(spark, df, name, grain)
            report[stage]["tables"][name] = summary
            require_checks(summary, name)
        monthly = report[stage]["tables"]["monthly_performance"]["by_month"]
        report[stage]["baseline"] = reconcile_baseline(monthly, baseline)
        report[stage]["baseline"]["checks"]["source_total_matches_monthly"] = (
            report["input"]["row_count"] == sum(row["total_flights"] for row in monthly)
        )
        report[stage]["baseline"]["checks"]["one_monthly_row_per_baseline_month"] = (
            report[stage]["tables"]["monthly_performance"]["row_count"] == len(baseline["by_month"])
        )
        require_checks(report[stage]["baseline"], "Phase 2 reconciliation")
        for name, summary in report[stage]["tables"].items():
            summary["checks"].update(reconcile_months(summary["by_month"], monthly))
            require_checks(summary, name + " monthly reconciliation")
        report[stage]["status"] = "PASS"

        stage = "write"
        report[stage]["status"] = "RUNNING"
        args.output_dir.mkdir(parents=True, exist_ok=False)
        for name, df in tables.items():
            report[stage]["tables"][name] = "RUNNING"
            df.coalesce(1).write.mode("errorifexists").option("compression", "snappy").parquet(
                (args.output_dir / name).resolve().as_posix()
            )
            report[stage]["tables"][name] = "PASS"
        report[stage]["status"] = "PASS"

        stage = "read_back"
        report[stage]["status"] = "RUNNING"
        for name, original in tables.items():
            report[stage]["tables"][name] = {"status": "RUNNING"}
            loaded = spark.read.parquet((args.output_dir / name).resolve().as_posix())
            summary = summarize_table(spark, loaded, "read_back_" + name, TABLE_GRAINS[name])
            report[stage]["tables"][name] = summary
            require_checks(summary, name + " read-back contract")
            summary["status"] = "RUNNING"
            summary["checks"]["row_count_unchanged"] = (
                summary["row_count"] == report["pre_write"]["tables"][name]["row_count"]
            )
            summary["checks"].update(reconcile_months(summary["by_month"], monthly))
            ordered = loaded.select(*original.columns)
            differences = original.exceptAll(ordered).unionByName(ordered.exceptAll(original))
            summary["checks"]["all_values_unchanged"] = differences.limit(1).count() == 0
            require_checks(summary, name + " read-back reconciliation")
            # Business queries use validated read-back data, not an extra persisted table.
            loaded.createOrReplaceTempView(name)
        report[stage]["status"] = "PASS"

        stage = "mvp_regression"
        report[stage]["status"] = "RUNNING"
        for name, grain in TABLE_GRAINS.items():
            reference_path = args.mvp_analytics_dir / name
            report[stage]["tables"][name] = {"status": "RUNNING"}
            reference = spark.read.parquet(reference_path.resolve().as_posix())
            # Compare the validated read-back table, not an unverified in-memory result.
            result = compare_mvp_table(spark.table(name), reference, grain)
            result["reference_path"] = reference_path.resolve().as_posix()
            report[stage]["tables"][name] = result
            require_checks(result, name + " January-March regression")
        report[stage]["status"] = "PASS"

        stage = "business_queries"
        report[stage]["status"] = "RUNNING"
        for filename in BUSINESS_QUERIES:
            report[stage]["queries"][filename] = "RUNNING"
            result = spark.sql(
                (SQL_DIR / filename).read_text(encoding="utf-8"),
                args={"min_completed_flights": args.min_completed_flights},
            )
            print(f"\n{filename} (bounded preview; interpretations pending owner review)")
            result.show(args.preview_rows, truncate=False)
            report[stage]["queries"][filename] = "PREVIEW_EXECUTED"
        report[stage]["status"] = "PREVIEW_EXECUTED"
        report["overall_status"] = "PASS"
    except Exception as error:
        report["overall_status"] = "FAIL"
        report["failure_stage"] = stage
        report[stage]["status"] = "FAIL"
        for collection in ("tables", "queries"):
            for name, value in report[stage].get(collection, {}).items():
                if isinstance(value, dict) and value.get("status") == "RUNNING":
                    value["status"] = "FAIL"
                elif value == "RUNNING":
                    report[stage][collection][name] = "FAIL"
        report["errors"].append({"type": type(error).__name__, "message": str(error)})
        raise
    finally:
        try:
            write_report(report, args.validation_output)
            print(f"\nPhase 3A: {report['overall_status']}; report: {args.validation_output}")
        finally:
            if shared_summary is not None:
                shared_summary.unpersist()
            if spark is not None:
                spark.stop()


if __name__ == "__main__":
    main()
