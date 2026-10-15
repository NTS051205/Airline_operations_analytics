"""Inspect completed raw flights with missing arrival fields; never repair data."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from functools import reduce
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession, functions as F

from airline_analytics.ingestion import load_raw_csvs, validate_input_files
from airline_analytics.quality_checks import is_missing
from airline_analytics.schema import DATASET_SCOPE, EXPECTED_FILE_METADATA


ROOT = Path(__file__).resolve().parents[1]
ARRIVAL_FIELDS = ("ARR_TIME", "ARR_DELAY", "ARR_DEL15")
MISSING_FLAGS = tuple(f"missing_{name}" for name in ARRIVAL_FIELDS)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", type=Path, default=ROOT / "data/raw")
    parser.add_argument(
        "--output", type=Path,
        default=ROOT / "artifacts/phase1_2025/arrival_missing_diagnostic.json",
    )
    parser.add_argument("--max-rows", type=int, default=100,
                        help="Maximum detailed raw rows exported, from 1 to 1000.")
    args = parser.parse_args()
    if not 1 <= args.max_rows <= 1000:
        parser.error("--max-rows must be between 1 and 1000")
    output, input_dir = args.output.resolve(), args.input_dir.resolve()
    if output.exists():
        raise FileExistsError(f"Diagnostic output already exists: {output}")
    if not output.is_relative_to(ROOT / "artifacts") or output.suffix != ".json":
        parser.error("Choose a new .json file below this project's artifacts directory")
    if output.is_relative_to(input_dir) or input_dir.is_relative_to(output):
        parser.error("Diagnostic output must not overlap raw input")
    return args


def select_suspects(raw: DataFrame) -> DataFrame:
    """Use the exact completed/missing definitions from Phase 1 profiling."""
    completed = (F.col("CANCELLED").cast("double") == 0.0) & (
        F.col("DIVERTED").cast("double") == 0.0
    )
    any_missing = reduce(
        lambda left, right: left | right,
        (is_missing(name) for name in ARRIVAL_FIELDS),
    )
    return raw.filter(completed & any_missing).select(
        "*", *[is_missing(name).alias(flag)
               for name, flag in zip(ARRIVAL_FIELDS, MISSING_FLAGS)]
    )


def collect_diagnostic(suspects: DataFrame, max_rows: int) -> dict:
    """Counts cover all matches; only the detailed row export is bounded."""
    patterns = [row.asDict() for row in (
        suspects.groupBy("_source_file", "_source_year", "_source_month", *MISSING_FLAGS)
        .count().orderBy("_source_file", *MISSING_FLAGS).collect()
    )]
    affected_rows = sum(row["count"] for row in patterns)
    missing_counts = {
        name: sum(row["count"] for row in patterns if row[flag])
        for name, flag in zip(ARRIVAL_FIELDS, MISSING_FLAGS)
    }
    all_three = sum(
        row["count"] for row in patterns if all(row[flag] for flag in MISSING_FLAGS)
    )
    details = [row.asDict() for row in (
        suspects.orderBy("_source_file", "FL_DATE", "OP_UNIQUE_CARRIER",
                         "OP_CARRIER_FL_NUM", "ORIGIN", "DEST", "CRS_DEP_TIME")
        .limit(max_rows).collect()
    )]
    return {
        "affected_rows": affected_rows,
        "completed_missing_counts": missing_counts,
        "rows_missing_all_three": all_three,
        "by_file_and_missing_pattern": patterns,
        "exported_rows": len(details),
        "rows_truncated": affected_rows > len(details),
        "raw_rows": details,
    }


def main() -> None:
    args = parse_args()
    paths = [args.input_dir / name for name in EXPECTED_FILE_METADATA]
    print("Validating all configured input files before diagnosis...")
    metadata = validate_input_files(paths)
    spark = (SparkSession.builder.appName("airline-arrival-missing-diagnostic")
             .config("spark.sql.shuffle.partitions", "8")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.ansi.enabled", "false")
             .config("spark.sql.csv.parser.columnPruning.enabled", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    suspects = None
    try:
        # Cache only matching rows, not the full annual dataset.
        suspects = select_suspects(load_raw_csvs(spark, paths)).persist(
            StorageLevel.MEMORY_AND_DISK
        )
        result = collect_diagnostic(suspects, args.max_rows)
        report = {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "scope": DATASET_SCOPE,
            "review_status": "PENDING_OWNER_REVIEW",
            "runtime": {"spark_version": spark.version,
                        "master": spark.sparkContext.master},
            "input_files": metadata,
            "selection": "CANCELLED=0 AND DIVERTED=0; any arrival field NULL/blank",
            "max_exported_rows": args.max_rows,
            **result,
            "limitations": [
                "Diagnostic counts are not data-quality approval.",
                "Raw values are retained as parsed by the existing CSV reader.",
                "No imputation, deletion, status change or KPI adjustment is applied.",
                "Cause of missingness cannot be inferred from these rows alone.",
            ],
        }
        args.output.parent.mkdir(parents=True, exist_ok=True)
        with args.output.open("x", encoding="utf-8") as handle:
            json.dump(report, handle, indent=2, ensure_ascii=False, allow_nan=False)
        print(json.dumps(result, indent=2, ensure_ascii=False))
        print(f"Diagnostic report: {args.output.resolve()}")
        print("Stop here: review these rows before cleaning.")
    finally:
        if suspects is not None:
            suspects.unpersist()
        spark.stop()


if __name__ == "__main__":
    main()
