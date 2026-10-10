"""Small user-run Parquet write/read smoke test for local Windows Spark."""

from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Write and read a three-row partitioned Parquet sample."
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("artifacts/phase2/parquet_smoke_test"),
        help="New test output directory; existing paths are refused.",
    )
    return parser.parse_args()


def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder.appName("airline-phase2-parquet-smoke-test")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def main() -> None:
    args = parse_args()
    if args.output_dir.exists():
        raise FileExistsError(
            f"Smoke-test output already exists: {args.output_dir}"
        )

    schema = StructType(
        [
            StructField("YEAR", IntegerType(), nullable=False),
            StructField("MONTH", IntegerType(), nullable=False),
            StructField("CRS_DEP_TIME", StringType(), nullable=True),
        ]
    )
    rows = [(2025, 1, "0015"), (2025, 1, "2400"), (2025, 2, None)]

    spark = create_spark_session()
    try:
        sample_df = spark.createDataFrame(rows, schema=schema)
        (
            sample_df.repartition(2, "YEAR", "MONTH")
            .write.mode("errorifexists")
            .option("compression", "snappy")
            .partitionBy("YEAR", "MONTH")
            .parquet(args.output_dir.resolve().as_posix())
        )

        read_back = spark.read.parquet(args.output_dir.resolve().as_posix())
        actual_types = {
            field.name: field.dataType.simpleString()
            for field in read_back.schema.fields
        }
        expected_types = {
            "YEAR": "int",
            "MONTH": "int",
            "CRS_DEP_TIME": "string",
        }
        actual_times = {
            row["CRS_DEP_TIME"]
            for row in read_back.select("CRS_DEP_TIME").collect()
        }

        if read_back.count() != 3:
            raise AssertionError("Parquet read-back row count is not 3")
        if actual_types != expected_types:
            raise AssertionError(
                f"Parquet read-back types differ: {actual_types}"
            )
        if actual_times != {"0015", "2400", None}:
            raise AssertionError(
                f"HHmm strings were not preserved: {actual_times}"
            )

        print("PARQUET SMOKE TEST: PASS")
        print("Rows: 3")
        print(f"Types: {actual_types}")
        print(f"Output retained at: {args.output_dir.resolve()}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
