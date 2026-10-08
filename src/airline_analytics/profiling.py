"""Data profiling metrics for the confirmed BTS Phase 1 raw schema."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from airline_analytics.quality_checks import (
    count_when,
    is_missing,
    is_present,
    profile_anomalies,
    profile_duplicates,
)
from airline_analytics.schema import (
    DOUBLE_COLUMNS,
    EXPECTED_COLUMNS,
    FLIGHT_DATE_PATTERN,
    INTEGER_COLUMNS,
    LOGICAL_TYPES,
)


def profile_file_level_metrics(df: DataFrame) -> dict[str, Any]:
    """Collect row counts, missingness, and statuses in one per-file action."""
    cancelled = F.col("CANCELLED").cast("double")
    diverted = F.col("DIVERTED").cast("double")
    status_conditions = {
        "completed": (cancelled == 0.0) & (diverted == 0.0),
        "cancelled": (cancelled == 1.0) & (diverted == 0.0),
        "diverted": (cancelled == 0.0) & (diverted == 1.0),
        "invalid_both_flags": (cancelled == 1.0) & (diverted == 1.0),
        "invalid_or_missing_flags": cancelled.isNull()
        | diverted.isNull()
        | (~cancelled.isin(0.0, 1.0))
        | (~diverted.isin(0.0, 1.0)),
    }
    aggregations: list[Column] = [F.count(F.lit(1)).alias("_row_count")]
    aggregations.extend(
        count_when(is_missing(column)).alias(f"missing__{column}")
        for column in EXPECTED_COLUMNS
    )
    aggregations.extend(
        count_when(condition).alias(f"status__{name}")
        for name, condition in status_conditions.items()
    )

    rows = (
        df.groupBy("_source_file", "_source_year", "_source_month")
        .agg(*aggregations)
        .orderBy("_source_year", "_source_month")
        .collect()
    )

    file_counts: list[dict[str, Any]] = []
    missing_by_file: dict[str, list[dict[str, Any]]] = {}
    status_by_file: dict[str, dict[str, int]] = {}
    overall_missing = {column: 0 for column in EXPECTED_COLUMNS}
    overall_status = {name: 0 for name in status_conditions}

    for row in rows:
        values = row.asDict()
        file_name = values["_source_file"]
        row_count = int(values["_row_count"])
        file_counts.append(
            {
                "file_name": file_name,
                "source_year": int(values["_source_year"]),
                "source_month": int(values["_source_month"]),
                "row_count": row_count,
            }
        )

        missing_by_file[file_name] = []
        for column in EXPECTED_COLUMNS:
            missing_count = int(values[f"missing__{column}"])
            overall_missing[column] += missing_count
            missing_by_file[file_name].append(
                {
                    "column": column,
                    "missing_count": missing_count,
                    "missing_percent": round(
                        (missing_count / row_count * 100) if row_count else 0.0, 4
                    ),
                }
            )

        file_status = {
            name: int(values[f"status__{name}"]) for name in status_conditions
        }
        for name, count in file_status.items():
            overall_status[name] += count
        file_status["reconciled_total"] = sum(file_status.values())
        status_by_file[file_name] = file_status

    total_rows = sum(item["row_count"] for item in file_counts)
    missing_overall = [
        {
            "column": column,
            "missing_count": overall_missing[column],
            "missing_percent": round(
                (overall_missing[column] / total_rows * 100) if total_rows else 0.0,
                4,
            ),
        }
        for column in EXPECTED_COLUMNS
    ]
    overall_status["reconciled_total"] = sum(overall_status.values())

    return {
        "total_rows": total_rows,
        "file_counts": file_counts,
        "missing_values": {
            "overall": missing_overall,
            "by_file": dict(sorted(missing_by_file.items())),
        },
        "flight_status": {
            "overall": overall_status,
            "by_file": dict(sorted(status_by_file.items())),
            "reconciles_to_row_count": overall_status["reconciled_total"]
            == total_rows,
        },
    }


def profile_type_validation(df: DataFrame) -> dict[str, Any]:
    """Collect cast failures and numeric ranges in one Spark aggregation."""
    target_expressions: dict[str, Column] = {
        column: F.col(column).cast("integer") for column in INTEGER_COLUMNS
    }
    target_expressions.update(
        {column: F.col(column).cast("double") for column in DOUBLE_COLUMNS}
    )
    target_expressions["FL_DATE"] = F.to_timestamp(
        F.col("FL_DATE"), FLIGHT_DATE_PATTERN
    )

    aggregations: list[Column] = []
    for column, expression in target_expressions.items():
        aggregations.append(
            count_when(is_present(column) & expression.isNull()).alias(
                f"{column}__cast_failures"
            )
        )
        if column in INTEGER_COLUMNS or column in DOUBLE_COLUMNS:
            aggregations.extend(
                [
                    F.min(expression).alias(f"{column}__min"),
                    F.max(expression).alias(f"{column}__max"),
                ]
            )

    values = df.agg(*aggregations).first().asDict()
    return {
        "cast_failures": {
            column: int(values[f"{column}__cast_failures"])
            for column in target_expressions
        },
        "numeric_ranges": {
            column: {
                "min": values[f"{column}__min"],
                "max": values[f"{column}__max"],
            }
            for column in (*INTEGER_COLUMNS, *DOUBLE_COLUMNS)
        },
    }


def profile_domain_summary(df: DataFrame) -> dict[str, Any]:
    cardinality_columns = (
        "OP_UNIQUE_CARRIER",
        "TAIL_NUM",
        "OP_CARRIER_FL_NUM",
        "ORIGIN",
        "DEST",
    )
    cardinalities = df.agg(
        *[
            F.countDistinct(column).alias(column) for column in cardinality_columns
        ]
    ).first().asDict()

    distribution_columns = (
        "YEAR",
        "MONTH",
        "DAY_OF_WEEK",
        "DEP_DEL15",
        "ARR_DEL15",
        "CANCELLED",
        "DIVERTED",
        "CANCELLATION_CODE",
    )
    long_values = (
        df.select(
            F.explode(
                F.array(
                    *[
                        F.struct(
                            F.lit(column).alias("column"),
                            F.col(column).alias("value"),
                        )
                        for column in distribution_columns
                    ]
                )
            ).alias("item")
        )
        .select("item.column", "item.value")
        .groupBy("column", "value")
        .count()
        .orderBy("column", F.desc("count"), F.asc_nulls_last("value"))
        .collect()
    )
    value_counts: dict[str, list[dict[str, Any]]] = {
        column: [] for column in distribution_columns
    }
    for row in long_values:
        value_counts[row["column"]].append(
            {"value": row["value"], "count": int(row["count"])}
        )

    return {
        "distinct_non_null": {
            column: int(cardinalities[column]) for column in cardinality_columns
        },
        "value_counts": value_counts,
    }


def build_profile_report(
    df: DataFrame,
    input_paths: Iterable[Path],
    verified_file_metadata: list[dict[str, Any]],
) -> dict[str, Any]:
    """Run Phase 1 metrics without modifying any raw values."""
    file_level = profile_file_level_metrics(df)
    type_validation = profile_type_validation(df)
    status_profile = file_level["flight_status"]
    return {
        "scope": "BTS Reporting Carrier On-Time Performance, 2025-01 to 2025-03",
        "input_files": verified_file_metadata,
        "input_paths": [str(path.resolve()) for path in input_paths],
        "header": {
            "column_count": len(EXPECTED_COLUMNS),
            "columns": list(EXPECTED_COLUMNS),
            "logical_types": LOGICAL_TYPES,
        },
        "row_counts": {
            "total": file_level["total_rows"],
            "by_file": file_level["file_counts"],
        },
        "missing_values": file_level["missing_values"],
        "cast_failures": type_validation["cast_failures"],
        "duplicates": profile_duplicates(df),
        "flight_status": status_profile["overall"],
        "flight_status_by_file": status_profile["by_file"],
        "flight_status_reconciles_to_row_count": status_profile[
            "reconciles_to_row_count"
        ],
        "domain_summary": profile_domain_summary(df),
        "numeric_ranges": type_validation["numeric_ranges"],
        "anomaly_counts": profile_anomalies(df),
    }
