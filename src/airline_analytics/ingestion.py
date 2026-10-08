"""Raw CSV validation and loading for Phase 1 profiling."""

from __future__ import annotations

import csv
import hashlib
import re
from pathlib import Path
from typing import Any, Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from airline_analytics.schema import EXPECTED_COLUMNS, EXPECTED_FILE_METADATA


def read_csv_header(path: Path) -> list[str]:
    """Read one CSV header without loading the data rows."""
    with path.open("r", encoding="utf-8-sig", newline="") as csv_file:
        return next(csv.reader(csv_file))


def calculate_sha256(path: Path, chunk_size: int = 1024 * 1024) -> str:
    """Calculate a file hash in fixed-size chunks."""
    digest = hashlib.sha256()
    with path.open("rb") as raw_file:
        while chunk := raw_file.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest().upper()


def validate_input_files(paths: Sequence[Path]) -> list[dict[str, Any]]:
    """Validate filenames, sizes, hashes, and exact column order."""
    expected_names = set(EXPECTED_FILE_METADATA)
    actual_names = {path.name for path in paths}
    if actual_names != expected_names:
        missing = sorted(expected_names - actual_names)
        unexpected = sorted(actual_names - expected_names)
        raise ValueError(
            f"Input file set does not match Phase 1 scope. "
            f"Missing={missing}; unexpected={unexpected}"
        )

    metadata: list[dict[str, Any]] = []
    for path in sorted(paths, key=lambda item: item.name):
        if not path.is_file():
            raise FileNotFoundError(f"CSV file not found: {path}")

        expected = EXPECTED_FILE_METADATA[path.name]
        size_bytes = path.stat().st_size
        if size_bytes != expected["size_bytes"]:
            raise ValueError(
                f"Unexpected size for {path.name}: {size_bytes}; "
                f"expected {expected['size_bytes']}"
            )

        header = read_csv_header(path)
        if tuple(header) != EXPECTED_COLUMNS:
            missing = [column for column in EXPECTED_COLUMNS if column not in header]
            unexpected = [column for column in header if column not in EXPECTED_COLUMNS]
            raise ValueError(
                f"Header mismatch for {path.name}. Missing={missing}; "
                f"unexpected={unexpected}; order_matches=False"
            )

        sha256 = calculate_sha256(path)
        expected_sha256 = str(expected["sha256"])
        if sha256.casefold() != expected_sha256.casefold():
            raise ValueError(
                f"SHA-256 mismatch for {path.name}: {sha256}; "
                f"expected {expected_sha256}"
            )

        metadata.append(
            {
                "file_name": path.name,
                "size_bytes": size_bytes,
                "sha256": sha256,
                "header_column_count": len(header),
            }
        )

    return metadata


def _extract_source_period(path: Path) -> tuple[int, int]:
    match = re.search(r"_(\d{4})_(\d{2})\.csv$", path.name, re.IGNORECASE)
    if not match:
        raise ValueError(f"Cannot extract year/month from filename: {path.name}")
    return int(match.group(1)), int(match.group(2))


def load_raw_csvs(spark: SparkSession, paths: Sequence[Path]) -> DataFrame:
    """Load every raw field as string and attach source-file metadata."""
    raw_schema = StructType(
        [StructField(column, StringType(), nullable=True) for column in EXPECTED_COLUMNS]
    )
    frames: list[DataFrame] = []
    for path in sorted(paths, key=lambda item: item.name):
        source_year, source_month = _extract_source_period(path)
        frame = (
            spark.read.schema(raw_schema)
            .option("header", True)
            .option("inferSchema", False)
            .option("enforceSchema", False)
            .option("mode", "FAILFAST")
            .option("encoding", "UTF-8")
            .csv(path.resolve().as_posix())
            .select(*EXPECTED_COLUMNS)
            .withColumn("_source_file", F.lit(path.name))
            .withColumn("_source_year", F.lit(source_year))
            .withColumn("_source_month", F.lit(source_month))
        )
        frames.append(frame)

    combined = frames[0]
    for frame in frames[1:]:
        combined = combined.unionByName(frame)
    return combined
