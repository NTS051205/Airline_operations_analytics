"""Command-line entry point for BTS raw-data profiling."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pyspark import StorageLevel
from pyspark.sql import SparkSession

from airline_analytics.ingestion import load_raw_csvs, validate_input_files
from airline_analytics.profiling import build_profile_report
from airline_analytics.schema import EXPECTED_FILE_METADATA


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Profile the three verified BTS raw CSV files for Phase 1."
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=Path("data/raw"),
        help="Directory containing the three verified monthly CSV files.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("artifacts/phase1/profile_summary.json"),
        help="JSON path for the complete profiling results.",
    )
    return parser.parse_args()


def create_spark_session() -> SparkSession:
    """Create a conservative local Spark session for a 12 GB laptop."""
    spark = (
        SparkSession.builder.appName("airline-phase1-profiling")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.sql.csv.parser.columnPruning.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def write_report(report: dict[str, Any], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        **report,
    }
    with output_path.open("w", encoding="utf-8") as output_file:
        json.dump(payload, output_file, indent=2, ensure_ascii=False)


def print_summary(report: dict[str, Any], output_path: Path) -> None:
    print("\nPHASE 1 PROFILING SUMMARY")
    print(f"Total rows: {report['row_counts']['total']:,}")
    for item in report["row_counts"]["by_file"]:
        print(f"  {item['file_name']}: {item['row_count']:,}")

    print("Flight status:")
    for status, count in report["flight_status"].items():
        print(f"  {status}: {count:,}")
    print(
        "  reconciles_to_row_count: "
        f"{report['flight_status_reconciles_to_row_count']}"
    )

    duplicates = report["duplicates"]
    print(
        "Exact full-row duplicates: "
        f"groups={duplicates['full_row_duplicate_groups']:,}; "
        f"rows_in_groups={duplicates['full_row_duplicate_rows_in_groups']:,}; "
        f"excess_rows={duplicates['full_row_duplicate_excess']:,}"
    )
    print(
        "Candidate-key collisions: "
        f"groups={duplicates['candidate_key_duplicate_groups']:,}; "
        "rows_in_groups="
        f"{duplicates['candidate_key_duplicate_rows_in_groups']:,}; "
        f"excess_rows={duplicates['candidate_key_duplicate_excess']:,}; "
        f"incomplete_key_rows={duplicates['candidate_key_incomplete_rows']:,}"
    )

    print("Non-zero anomaly checks:")
    found_anomaly = False
    for category, checks in report["anomaly_counts"].items():
        nonzero_checks = {name: count for name, count in checks.items() if count > 0}
        if not nonzero_checks:
            continue
        found_anomaly = True
        print(f"  {category}:")
        for name, count in nonzero_checks.items():
            print(f"    {name}: {count:,}")
    if not found_anomaly:
        print("  none")
    print(f"Full JSON report: {output_path.resolve()}")


def main() -> None:
    args = parse_args()
    input_paths = [args.input_dir / name for name in EXPECTED_FILE_METADATA]

    print("Validating filenames, sizes, headers, and SHA-256 hashes...")
    file_metadata = validate_input_files(input_paths)
    print("Input validation passed. Starting Spark profiling...")

    spark = create_spark_session()
    raw_df = None
    try:
        raw_df = load_raw_csvs(spark, input_paths).persist(
            StorageLevel.MEMORY_AND_DISK
        )
        report = build_profile_report(raw_df, input_paths, file_metadata)
        write_report(report, args.output)
        print_summary(report, args.output)
    finally:
        if raw_df is not None:
            raw_df.unpersist()
        spark.stop()


if __name__ == "__main__":
    main()
