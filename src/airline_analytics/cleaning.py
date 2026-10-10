"""Phase 2 BTS cleaning transformations and target schema contract."""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, DoubleType, IntegerType, StringType

from airline_analytics.schema import (
    BINARY_FLAG_COLUMNS,
    DOUBLE_COLUMNS,
    EXPECTED_COLUMNS,
    FLIGHT_DATE_PATTERN,
    INTEGER_COLUMNS,
)


DOUBLE_MEASURE_COLUMNS = tuple(
    column for column in DOUBLE_COLUMNS if column not in BINARY_FLAG_COLUMNS
)

CLEANED_COLUMNS = (*EXPECTED_COLUMNS, "_source_file", "flight_status")
VALID_FLIGHT_STATUSES = ("completed", "cancelled", "diverted", "invalid")


def normalized_text(column_name: str) -> Column:
    """Trim a raw string and convert null/whitespace-only values to null."""
    value = F.col(column_name)
    trimmed = F.trim(value)
    return F.when(
        value.isNull() | (trimmed == ""),
        F.lit(None).cast(StringType()),
    ).otherwise(trimmed)


def safe_binary_flag(column_name: str) -> Column:
    """Convert numeric 0/1 strings to int without truncating other numbers."""
    text = normalized_text(column_name)
    numeric = text.cast(DoubleType())
    return (
        F.when(text.isNull(), F.lit(None).cast(IntegerType()))
        .when(numeric.isin(0.0, 1.0), numeric.cast(IntegerType()))
        .otherwise(F.lit(None).cast(IntegerType()))
    )


def typed_column(column_name: str) -> Column:
    """Return the Phase 2 target expression for one BTS source column."""
    text = normalized_text(column_name)
    if column_name in BINARY_FLAG_COLUMNS:
        return safe_binary_flag(column_name)
    if column_name in INTEGER_COLUMNS:
        return text.cast(IntegerType())
    if column_name in DOUBLE_MEASURE_COLUMNS:
        return text.cast(DoubleType())
    if column_name == "FL_DATE":
        return F.to_date(F.to_timestamp(text, FLIGHT_DATE_PATTERN))
    return text.cast(StringType())


def clean_flights(raw_df: DataFrame) -> DataFrame:
    """Apply typed casts and status logic without filtering any raw record."""
    typed_df = raw_df.select(
        *[
            typed_column(column).alias(column)
            for column in EXPECTED_COLUMNS
        ],
        normalized_text("_source_file").alias("_source_file"),
    )

    flight_status = (
        F.when(
            (F.col("CANCELLED") == 0) & (F.col("DIVERTED") == 0),
            F.lit("completed"),
        )
        .when(
            (F.col("CANCELLED") == 1) & (F.col("DIVERTED") == 0),
            F.lit("cancelled"),
        )
        .when(
            (F.col("CANCELLED") == 0) & (F.col("DIVERTED") == 1),
            F.lit("diverted"),
        )
        .otherwise(F.lit("invalid"))
    )
    return typed_df.withColumn("flight_status", flight_status).select(
        *CLEANED_COLUMNS
    )


def expected_cleaned_types() -> dict[str, str]:
    """Return the expected Spark simpleString type for every cleaned column."""
    expected = {column: "string" for column in EXPECTED_COLUMNS}
    expected.update({column: "int" for column in INTEGER_COLUMNS})
    expected.update({column: "double" for column in DOUBLE_MEASURE_COLUMNS})
    expected.update({column: "int" for column in BINARY_FLAG_COLUMNS})
    expected["FL_DATE"] = "date"
    expected["_source_file"] = "string"
    expected["flight_status"] = "string"
    return expected
