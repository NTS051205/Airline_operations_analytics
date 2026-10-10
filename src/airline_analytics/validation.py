"""Compact Phase 2 validation metrics for cleaned BTS flight data."""

from __future__ import annotations

from typing import Any

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

from airline_analytics.cleaning import (
    DOUBLE_MEASURE_COLUMNS,
    VALID_FLIGHT_STATUSES,
    expected_cleaned_types,
    normalized_text,
    typed_column,
)
from airline_analytics.quality_checks import count_when
from airline_analytics.schema import (
    BINARY_FLAG_COLUMNS,
    INTEGER_COLUMNS,
    TIME_COLUMNS,
    VALID_HHMM_PATTERN,
)


def _raw_status_conditions() -> dict[str, Column]:
    cancelled = normalized_text("CANCELLED").cast(DoubleType())
    diverted = normalized_text("DIVERTED").cast(DoubleType())
    conditions = {
        "completed": (cancelled == 0.0) & (diverted == 0.0),
        "cancelled": (cancelled == 1.0) & (diverted == 0.0),
        "diverted": (cancelled == 0.0) & (diverted == 1.0),
    }
    any_valid_status = F.coalesce(
        conditions["completed"]
        | conditions["cancelled"]
        | conditions["diverted"],
        F.lit(False),
    )
    conditions["invalid"] = ~any_valid_status
    return conditions


def collect_raw_summary(raw_df: DataFrame) -> dict[str, Any]:
    """Collect raw month and flight-status counts in one grouped action."""
    status_conditions = _raw_status_conditions()
    rows = (
        raw_df.groupBy("_source_year", "_source_month")
        .agg(
            F.count(F.lit(1)).alias("row_count"),
            *[
                count_when(condition).alias(f"status__{status}")
                for status, condition in status_conditions.items()
            ],
        )
        .orderBy("_source_year", "_source_month")
        .collect()
    )

    by_month: list[dict[str, int]] = []
    status_counts = {status: 0 for status in VALID_FLIGHT_STATUSES}
    for row in rows:
        by_month.append(
            {
                "year": int(row["_source_year"]),
                "month": int(row["_source_month"]),
                "row_count": int(row["row_count"]),
            }
        )
        for status in VALID_FLIGHT_STATUSES:
            status_counts[status] += int(row[f"status__{status}"])

    return {
        "total_rows": sum(item["row_count"] for item in by_month),
        "by_month": by_month,
        "flight_status": status_counts,
    }


def collect_transformation_issues(raw_df: DataFrame) -> dict[str, Any]:
    """Measure introduced cast nulls, binary-domain errors, and HHmm errors."""
    cast_columns = (
        *INTEGER_COLUMNS,
        "FL_DATE",
        *DOUBLE_MEASURE_COLUMNS,
        *BINARY_FLAG_COLUMNS,
    )
    aggregations: list[Column] = []
    for column in cast_columns:
        source = normalized_text(column)
        target = typed_column(column)
        aggregations.extend(
            [
                count_when(source.isNull()).alias(f"{column}__source_nulls"),
                count_when(target.isNull()).alias(f"{column}__target_nulls"),
                count_when(source.isNotNull() & target.isNull()).alias(
                    f"{column}__introduced_nulls"
                ),
            ]
        )

    for column in BINARY_FLAG_COLUMNS:
        source = normalized_text(column)
        numeric = source.cast(DoubleType())
        valid_binary = F.coalesce(numeric.isin(0.0, 1.0), F.lit(False))
        aggregations.append(
            count_when(source.isNotNull() & (~valid_binary)).alias(
                f"{column}__invalid_binary"
            )
        )

    for column in TIME_COLUMNS:
        source = normalized_text(column)
        aggregations.append(
            count_when(
                source.isNotNull() & (~source.rlike(VALID_HHMM_PATTERN))
            ).alias(f"{column}__invalid_hhmm")
        )

    values = raw_df.agg(*aggregations).first().asDict()
    per_column = {
        column: {
            "source_nulls": int(values[f"{column}__source_nulls"]),
            "target_nulls": int(values[f"{column}__target_nulls"]),
            "introduced_nulls": int(values[f"{column}__introduced_nulls"]),
        }
        for column in cast_columns
    }
    binary_invalid = {
        column: int(values[f"{column}__invalid_binary"])
        for column in BINARY_FLAG_COLUMNS
    }
    hhmm_invalid = {
        column: int(values[f"{column}__invalid_hhmm"])
        for column in TIME_COLUMNS
    }
    introduced_nulls_total = sum(
        item["introduced_nulls"] for item in per_column.values()
    )

    return {
        "per_column": per_column,
        "unexpected_cast_nulls_total": introduced_nulls_total,
        "binary_invalid_counts": binary_invalid,
        "hhmm_invalid_counts": hhmm_invalid,
        "hhmm_transformation": "trim only; values remain strings",
        "pass": (
            introduced_nulls_total == 0
            and sum(binary_invalid.values()) == 0
            and sum(hhmm_invalid.values()) == 0
        ),
    }


def collect_cleaned_summary(cleaned_df: DataFrame) -> dict[str, Any]:
    """Collect cleaned counts and row-level rule violations by month."""
    status_conditions = {
        status: F.col("flight_status") == status
        for status in VALID_FLIGHT_STATUSES
    }
    completed_arr_del15_invalid = status_conditions["completed"] & (
        F.col("ARR_DEL15").isNull()
        | (~F.col("ARR_DEL15").isin(0, 1))
    )
    non_completed_arr_del15_present = (
        F.col("flight_status") != "completed"
    ) & F.col("ARR_DEL15").isNotNull()

    aggregations: list[Column] = [F.count(F.lit(1)).alias("row_count")]
    aggregations.extend(
        count_when(condition).alias(f"status__{status}")
        for status, condition in status_conditions.items()
    )
    aggregations.extend(
        [
            count_when(
                F.col("_source_file").isNull()
                | (F.trim(F.col("_source_file")) == "")
            ).alias("source_file_nulls"),
            count_when(
                F.col("ARR_DEL15").isNotNull()
                & (~F.col("ARR_DEL15").isin(0, 1))
            ).alias("arr_del15_invalid_domain"),
            count_when(completed_arr_del15_invalid).alias(
                "completed_arr_del15_invalid"
            ),
            count_when(non_completed_arr_del15_present).alias(
                "non_completed_arr_del15_present"
            ),
        ]
    )
    for column in BINARY_FLAG_COLUMNS:
        aggregations.append(
            count_when(
                F.col(column).isNotNull() & (~F.col(column).isin(0, 1))
            ).alias(f"{column}__invalid_binary")
        )
    for column in TIME_COLUMNS:
        aggregations.append(
            count_when(
                F.col(column).isNotNull()
                & (~F.col(column).rlike(VALID_HHMM_PATTERN))
            ).alias(f"{column}__invalid_hhmm")
        )

    rows = (
        cleaned_df.groupBy("YEAR", "MONTH")
        .agg(*aggregations)
        .orderBy(F.asc_nulls_first("YEAR"), F.asc_nulls_first("MONTH"))
        .collect()
    )

    by_month: list[dict[str, int | None]] = []
    status_counts = {status: 0 for status in VALID_FLIGHT_STATUSES}
    source_file_nulls = 0
    arr_checks = {
        "invalid_domain_count": 0,
        "completed_missing_or_invalid_count": 0,
        "non_completed_labeled_count": 0,
    }
    binary_invalid = {column: 0 for column in BINARY_FLAG_COLUMNS}
    hhmm_invalid = {column: 0 for column in TIME_COLUMNS}

    for row in rows:
        by_month.append(
            {
                "year": int(row["YEAR"]) if row["YEAR"] is not None else None,
                "month": int(row["MONTH"])
                if row["MONTH"] is not None
                else None,
                "row_count": int(row["row_count"]),
            }
        )
        for status in VALID_FLIGHT_STATUSES:
            status_counts[status] += int(row[f"status__{status}"])
        source_file_nulls += int(row["source_file_nulls"])
        arr_checks["invalid_domain_count"] += int(
            row["arr_del15_invalid_domain"]
        )
        arr_checks["completed_missing_or_invalid_count"] += int(
            row["completed_arr_del15_invalid"]
        )
        arr_checks["non_completed_labeled_count"] += int(
            row["non_completed_arr_del15_present"]
        )
        for column in BINARY_FLAG_COLUMNS:
            binary_invalid[column] += int(row[f"{column}__invalid_binary"])
        for column in TIME_COLUMNS:
            hhmm_invalid[column] += int(row[f"{column}__invalid_hhmm"])

    total_rows = sum(item["row_count"] for item in by_month)
    status_sum = sum(status_counts.values())
    arr_checks["pass"] = all(value == 0 for value in arr_checks.values())
    return {
        "total_rows": total_rows,
        "by_month": by_month,
        "flight_status": status_counts,
        "status_sum": status_sum,
        "status_reconciles": status_sum == total_rows,
        "source_file_null_count": source_file_nulls,
        "binary_invalid_counts": binary_invalid,
        "hhmm_invalid_counts": hhmm_invalid,
        "arr_del15_checks": arr_checks,
    }


def collect_arr_del15_violation_samples(
    cleaned_df: DataFrame, limit: int = 5
) -> list[dict[str, Any]]:
    """Collect a small sample only when ARR_DEL15 row-level checks fail."""
    violation = (
        (F.col("flight_status") == "completed")
        & (
            F.col("ARR_DEL15").isNull()
            | (~F.col("ARR_DEL15").isin(0, 1))
        )
    ) | (
        (F.col("flight_status") != "completed")
        & F.col("ARR_DEL15").isNotNull()
    )
    rows = (
        cleaned_df.filter(violation)
        .select(
            "_source_file",
            F.date_format("FL_DATE", "yyyy-MM-dd").alias("FL_DATE"),
            "OP_UNIQUE_CARRIER",
            "OP_CARRIER_FL_NUM",
            "ORIGIN",
            "DEST",
            "CANCELLED",
            "DIVERTED",
            "flight_status",
            "ARR_DEL15",
        )
        .limit(limit)
        .collect()
    )
    return [row.asDict() for row in rows]


def compare_cleaned_schema(df: DataFrame) -> dict[str, Any]:
    """Compare names and types while ignoring order and nullable metadata."""
    expected = expected_cleaned_types()
    actual = {field.name: field.dataType.simpleString() for field in df.schema.fields}
    missing_columns = sorted(set(expected) - set(actual))
    extra_columns = sorted(set(actual) - set(expected))
    type_mismatches = [
        {
            "column": column,
            "expected": expected[column],
            "actual": actual[column],
        }
        for column in expected.keys() & actual.keys()
        if expected[column] != actual[column]
    ]
    return {
        "expected_types": expected,
        "actual_types": actual,
        "missing_columns": missing_columns,
        "extra_columns": extra_columns,
        "type_mismatches": sorted(
            type_mismatches, key=lambda item: item["column"]
        ),
        "pass": not missing_columns and not extra_columns and not type_mismatches,
    }
