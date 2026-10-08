"""Duplicate and data-quality rule checks for the confirmed BTS raw schema."""

from __future__ import annotations

from functools import reduce
from typing import Any

from pyspark import StorageLevel
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from airline_analytics.schema import (
    BINARY_FLAG_COLUMNS,
    BUSINESS_KEY_COLUMNS,
    DELAY_CAUSE_COLUMNS,
    EXPECTED_COLUMNS,
    EXPECTED_MONTHS,
    EXPECTED_YEAR,
    FLIGHT_DATE_PATTERN,
    TIME_COLUMNS,
    VALID_HHMM_PATTERN,
)


def is_missing(column_name: str) -> Column:
    """Return true for a Spark null or a blank/whitespace-only raw value."""
    column = F.col(column_name)
    return column.isNull() | (F.trim(column) == "")


def is_present(column_name: str) -> Column:
    """Return true when a raw value is neither null nor blank."""
    return ~is_missing(column_name)


def count_when(condition: Column) -> Column:
    """Build a null-safe count aggregation for a Boolean condition."""
    return F.coalesce(
        F.sum(F.when(condition, F.lit(1)).otherwise(F.lit(0))), F.lit(0)
    ).cast("long")


def profile_duplicates(df: DataFrame) -> dict[str, Any]:
    """Profile exact duplicates and candidate-key collisions without dropping rows."""
    exact_groups = df.groupBy(*EXPECTED_COLUMNS).count()
    exact_summary = exact_groups.agg(
        count_when(F.col("count") > 1).alias("duplicate_groups"),
        F.coalesce(
            F.sum(F.when(F.col("count") > 1, F.col("count")).otherwise(0)),
            F.lit(0),
        ).alias("rows_in_groups"),
        F.coalesce(
            F.sum(
                F.when(F.col("count") > 1, F.col("count") - 1).otherwise(0)
            ),
            F.lit(0),
        ).alias("extra_rows"),
    ).first()

    candidate_groups = (
        df.groupBy(*BUSINESS_KEY_COLUMNS)
        .count()
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    grouped_key_is_missing = reduce(
        lambda left, right: left | right,
        (is_missing(column) for column in BUSINESS_KEY_COLUMNS),
    )
    valid_duplicate_group = (~grouped_key_is_missing) & (F.col("count") > 1)

    try:
        candidate_summary = candidate_groups.agg(
            F.coalesce(
                F.sum(
                    F.when(grouped_key_is_missing, F.col("count")).otherwise(0)
                ),
                F.lit(0),
            ).alias("incomplete_key_rows"),
            count_when(valid_duplicate_group).alias("duplicate_groups"),
            F.coalesce(
                F.sum(
                    F.when(valid_duplicate_group, F.col("count")).otherwise(0)
                ),
                F.lit(0),
            ).alias("rows_in_groups"),
            F.coalesce(
                F.sum(
                    F.when(
                        valid_duplicate_group, F.col("count") - F.lit(1)
                    ).otherwise(0)
                ),
                F.lit(0),
            ).alias("extra_rows"),
        ).first()
        sample_rows = (
            candidate_groups.filter(valid_duplicate_group)
            .orderBy(F.desc("count"))
            .limit(10)
            .collect()
        )
    finally:
        candidate_groups.unpersist()

    return {
        "full_row_duplicate_groups": int(exact_summary["duplicate_groups"]),
        "full_row_duplicate_rows_in_groups": int(exact_summary["rows_in_groups"]),
        "full_row_duplicate_excess": int(exact_summary["extra_rows"]),
        "candidate_key_columns": list(BUSINESS_KEY_COLUMNS),
        "candidate_key_incomplete_rows": int(
            candidate_summary["incomplete_key_rows"]
        ),
        "candidate_key_duplicate_groups": int(
            candidate_summary["duplicate_groups"]
        ),
        "candidate_key_duplicate_rows_in_groups": int(
            candidate_summary["rows_in_groups"]
        ),
        "candidate_key_duplicate_excess": int(candidate_summary["extra_rows"]),
        "candidate_key_top_samples": [row.asDict() for row in sample_rows],
    }


def profile_anomalies(df: DataFrame) -> dict[str, dict[str, int]]:
    """Count confirmed invalid values separately from suspicious business rules."""
    year = F.col("YEAR").cast("integer")
    month = F.col("MONTH").cast("integer")
    day_of_week = F.col("DAY_OF_WEEK").cast("integer")
    cancelled = F.col("CANCELLED").cast("double")
    diverted = F.col("DIVERTED").cast("double")
    arr_delay = F.col("ARR_DELAY").cast("double")
    arr_delay_new = F.col("ARR_DELAY_NEW").cast("double")
    arr_del15 = F.col("ARR_DEL15").cast("double")
    dep_delay = F.col("DEP_DELAY").cast("double")
    dep_del15 = F.col("DEP_DEL15").cast("double")
    distance = F.col("DISTANCE").cast("double")
    flight_timestamp = F.to_timestamp(F.col("FL_DATE"), FLIGHT_DATE_PATTERN)
    bts_day_of_week = (
        F.pmod(F.dayofweek(flight_timestamp) + F.lit(5), F.lit(7)) + F.lit(1)
    )
    completed = (cancelled == 0.0) & (diverted == 0.0)

    confirmed_invalid: dict[str, Column] = {
        "year_outside_2025": is_present("YEAR") & (year != EXPECTED_YEAR),
        "month_outside_mvp_scope": is_present("MONTH")
        & (~month.isin(*EXPECTED_MONTHS)),
        "day_of_week_outside_1_7": is_present("DAY_OF_WEEK")
        & (~day_of_week.between(1, 7)),
        "unparseable_flight_date": is_present("FL_DATE")
        & flight_timestamp.isNull(),
        "flight_date_year_mismatch": flight_timestamp.isNotNull()
        & (F.year(flight_timestamp) != year),
        "flight_date_month_mismatch": flight_timestamp.isNotNull()
        & (F.month(flight_timestamp) != month),
        "day_of_week_mismatch_with_flight_date": flight_timestamp.isNotNull()
        & day_of_week.isNotNull()
        & (bts_day_of_week != day_of_week),
        "source_year_mismatch": year.isNotNull() & (year != F.col("_source_year")),
        "source_month_mismatch": month.isNotNull()
        & (month != F.col("_source_month")),
        "cancelled_and_diverted": (cancelled == 1.0) & (diverted == 1.0),
        "distance_nonpositive": distance.isNotNull() & (distance <= 0.0),
        "arr_del15_inconsistent_with_arr_delay": arr_delay.isNotNull()
        & arr_del15.isNotNull()
        & (
            ((arr_delay >= 15.0) & (arr_del15 != 1.0))
            | ((arr_delay < 15.0) & (arr_del15 != 0.0))
        ),
        "dep_del15_inconsistent_with_dep_delay": dep_delay.isNotNull()
        & dep_del15.isNotNull()
        & (
            ((dep_delay >= 15.0) & (dep_del15 != 1.0))
            | ((dep_delay < 15.0) & (dep_del15 != 0.0))
        ),
        "arr_delay_new_negative": arr_delay_new < 0.0,
        "arr_delay_new_inconsistent_with_arr_delay": arr_delay.isNotNull()
        & arr_delay_new.isNotNull()
        & (
            F.abs(arr_delay_new - F.greatest(arr_delay, F.lit(0.0)))
            > F.lit(0.0001)
        ),
    }

    for column in BINARY_FLAG_COLUMNS:
        numeric = F.col(column).cast("double")
        confirmed_invalid[f"{column.lower()}_outside_0_1"] = is_present(
            column
        ) & (~numeric.isin(0.0, 1.0))

    for column in TIME_COLUMNS:
        confirmed_invalid[f"{column.lower()}_invalid_hhmm"] = is_present(
            column
        ) & (~F.trim(F.col(column)).rlike(VALID_HHMM_PATTERN))

    for column in DELAY_CAUSE_COLUMNS:
        confirmed_invalid[f"{column.lower()}_negative"] = (
            F.col(column).cast("double") < 0.0
        )

    suspicious_business_rules: dict[str, Column] = {
        "distance_missing": is_missing("DISTANCE"),
        "origin_equals_destination": is_present("ORIGIN_AIRPORT_ID")
        & is_present("DEST_AIRPORT_ID")
        & (F.col("ORIGIN_AIRPORT_ID") == F.col("DEST_AIRPORT_ID")),
        "completed_missing_dep_time": completed & is_missing("DEP_TIME"),
        "completed_missing_arr_time": completed & is_missing("ARR_TIME"),
        "completed_missing_arr_delay": completed & is_missing("ARR_DELAY"),
        "completed_missing_arr_del15": completed & is_missing("ARR_DEL15"),
        "cancelled_missing_cancellation_code": (cancelled == 1.0)
        & is_missing("CANCELLATION_CODE"),
        "noncancelled_with_cancellation_code": (cancelled == 0.0)
        & is_present("CANCELLATION_CODE"),
        "cancelled_with_arr_time": (cancelled == 1.0) & is_present("ARR_TIME"),
        "diverted_with_arr_delay": (diverted == 1.0) & is_present("ARR_DELAY"),
        "delayed_completed_missing_all_delay_causes": completed
        & (arr_del15 == 1.0)
        & reduce(
            lambda left, right: left & right,
            (is_missing(column) for column in DELAY_CAUSE_COLUMNS),
        ),
    }

    all_rules = {**confirmed_invalid, **suspicious_business_rules}
    values = df.agg(
        *[count_when(condition).alias(name) for name, condition in all_rules.items()]
    ).first().asDict()
    return {
        "confirmed_invalid": {
            name: int(values[name]) for name in confirmed_invalid
        },
        "suspicious_business_rules": {
            name: int(values[name]) for name in suspicious_business_rules
        },
    }
