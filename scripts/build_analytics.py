"""Build four Phase 3A Spark SQL outputs and validate their Parquet round trip."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pyspark import StorageLevel
from pyspark.sql import SparkSession

from airline_analytics.analytics_validation import (
    TABLE_GRAINS,
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
    parser.add_argument("--input-dir", type=Path, default=PROJECT_ROOT / "data/processed/flights_cleaned")
    parser.add_argument("--phase2-report", type=Path, default=PROJECT_ROOT / "artifacts/phase2/validation_summary.json")
    parser.add_argument("--output-dir", type=Path, default=PROJECT_ROOT / "data/analytics")
    parser.add_argument("--validation-output", type=Path, default=PROJECT_ROOT / "artifacts/phase3/validation_summary.json")
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
    paths = [p.resolve() for p in (
        args.input_dir, args.phase2_report, args.output_dir, args.validation_output
    )]
    for index, left in enumerate(paths):
        for right in paths[index + 1:]:
            if left.is_relative_to(right) or right.is_relative_to(left):
                raise ValueError(f"Input/report/output paths must not overlap: {left}, {right}")
    if not args.input_dir.is_dir():
        raise FileNotFoundError(f"Cleaned Parquet directory not found: {args.input_dir}")
    if args.output_dir.exists():
        raise FileExistsError(f"Refusing existing analytics directory: {args.output_dir}")
    if args.validation_output.exists():
        raise FileExistsError(f"Refusing existing validation report: {args.validation_output}")


def load_baseline(path: Path) -> dict[str, Any]:
    """Read recorded Phase 2 evidence; do not rerun cleaning or hardcode results."""
    evidence = json.loads(path.read_text(encoding="utf-8-sig"))
    if evidence["phase"] != 2 or evidence["overall_status"] != "PASS" or evidence["errors"]:
        raise ValueError("Phase 2 evidence must record PASS without errors.")
    for stage in ("pre_write", "write", "read_back"):
        if evidence[stage]["status"] != "PASS":
            raise ValueError(f"Phase 2 {stage} is not PASS.")
    for stage in ("pre_write", "read_back"):
        checks = evidence[stage]["checks"]
        if not checks or not all(check["passed"] is True for check in checks):
            raise ValueError(f"Phase 2 {stage} contains missing/failed checks.")
    baseline = evidence["read_back"]["summary"]
    cleaned = evidence["pre_write"]["cleaned"]
    for key in ("total_rows", "by_month", "flight_status"):
        if baseline[key] != cleaned[key]:
            raise ValueError(f"Phase 2 pre-write/read-back disagree on {key}.")
    return {key: baseline[key] for key in ("total_rows", "by_month", "flight_status")}


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
        "output": {"directory": args.output_dir.resolve().as_posix(),
                   "format": "parquet", "compression": "snappy",
                   "partition_columns": [], "max_data_files_per_table": 1},
        "volume_heuristic": args.min_completed_flights,
        "preflight": {"status": "NOT_RUN"},
        "input": {"status": "NOT_RUN"},
        "pre_write": {"status": "NOT_RUN", "tables": {}},
        "write": {"status": "NOT_RUN", "tables": {}},
        "read_back": {"status": "NOT_RUN", "tables": {}},
        "business_queries": {"status": "NOT_RUN", "queries": {}},
        "errors": [],
        "limitations": [
            "PASS covers these checks, not every possible data-quality issue.",
            "No benchmark, causal analysis, long-term trend or ML evaluation.",
            "Business query previews are not reviewed business conclusions.",
            "Volume threshold is a heuristic, not a statistical confidence guarantee.",
            "Partial outputs are retained on failure; there is no automatic cleanup.",
        ],
    }


def write_report(report: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x", encoding="utf-8") as handle:
        json.dump(report, handle, indent=2, ensure_ascii=False, allow_nan=False)


def main() -> None:
    args = parse_args()
    # Preflight failures do not create directories or replace earlier evidence.
    check_paths(args)
    report = initial_report(args)
    spark = None
    shared_summary = None
    stage = "preflight"
    try:
        report[stage]["status"] = "RUNNING"
        baseline = load_baseline(args.phase2_report)
        report[stage] = {"status": "PASS", "phase2_baseline": baseline}
        spark = create_spark_session()
        report["runtime"] = {"spark_version": spark.version,
                             "master": spark.sparkContext.master,
                             "shuffle_partitions": spark.conf.get("spark.sql.shuffle.partitions")}
        stage = "input"
        report[stage]["status"] = "RUNNING"
        cleaned = spark.read.parquet(args.input_dir.resolve().as_posix())
        cleaned.createOrReplaceTempView("flights_cleaned")
        report[stage] = validate_input(spark, cleaned)
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
