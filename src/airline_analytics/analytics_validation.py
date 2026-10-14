"""Phase 3A checks for KPI denominators, aggregate grain and reconciliation."""

from __future__ import annotations

from math import isclose, isfinite
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from airline_analytics.cleaning import expected_cleaned_types
from airline_analytics.schema import EXPECTED_MONTHS, EXPECTED_YEAR


TABLE_GRAINS = {
    "monthly_performance": ("YEAR", "MONTH"),
    "airline_monthly_performance": ("YEAR", "MONTH", "OP_UNIQUE_CARRIER"),
    "origin_airport_monthly_performance": ("YEAR", "MONTH", "ORIGIN"),
    "route_monthly_performance": ("YEAR", "MONTH", "ORIGIN", "DEST"),
}
COUNT_COLUMNS = (
    "total_flights", "completed_flights", "cancelled_flights", "diverted_flights",
    "delayed_arrival_flights", "on_time_arrival_flights",
    "arrival_delay_observed_flights",
)
RATE_COMPONENTS = {
    "arrival_delay_rate": ("delayed_arrival_flights", "completed_flights"),
    "on_time_arrival_rate": ("on_time_arrival_flights", "completed_flights"),
    "cancellation_rate": ("cancelled_flights", "total_flights"),
    "diversion_rate": ("diverted_flights", "total_flights"),
}
# Regression tolerates only floating-point aggregation-order noise, never counts.
REGRESSION_REL_TOL = 1E-12
REGRESSION_ABS_TOL = 1E-9
MVP_YEAR = 2025
MVP_MONTHS = (1, 2, 3)


def schema_check(df: DataFrame, expected: dict[str, str]) -> dict[str, Any]:
    actual = {field.name: field.dataType.simpleString() for field in df.schema}
    return {
        "passed": actual == expected and len(df.columns) == len(expected),
        "expected": expected,
        "actual": actual,
    }


def violation_counts_sql(rules: dict[str, str]) -> str:
    """A NULL predicate is a violation, not a silently skipped SQL row."""
    return ",\n".join(
        f"COUNT(CASE WHEN NOT COALESCE(({rule}), FALSE) THEN 1 END) AS {name}"
        for name, rule in rules.items()
    )


def validate_input(spark: SparkSession, df: DataFrame, baseline: dict[str, Any]) -> dict[str, Any]:
    """Check the existing cleaned contract without recasting or dropping rows."""
    schema = schema_check(df, expected_cleaned_types())
    checks = {"cleaned_schema": schema["passed"]}
    result = {"checks": checks, "schema": schema}
    if not schema["passed"]:
        return result
    rules = {
        "invalid_period": f"YEAR = {EXPECTED_YEAR} AND MONTH IN "
                          f"({', '.join(str(month) for month in EXPECTED_MONTHS)})",
        "invalid_date": "FL_DATE IS NOT NULL AND YEAR(FL_DATE) = YEAR "
                        "AND MONTH(FL_DATE) = MONTH",
        "missing_grouping_keys": " AND ".join(
            f"{key} IS NOT NULL AND TRIM({key}) <> ''"
            for key in ("OP_UNIQUE_CARRIER", "ORIGIN", "DEST")
        ),
        "invalid_status": "flight_status IN ('completed', 'cancelled', 'diverted')",
        "status_flag_mismatch": """flight_status <=> CASE
            WHEN CANCELLED = 0 AND DIVERTED = 0 THEN 'completed'
            WHEN CANCELLED = 1 AND DIVERTED = 0 THEN 'cancelled'
            WHEN CANCELLED = 0 AND DIVERTED = 1 THEN 'diverted'
            ELSE 'invalid' END""",
        "invalid_arrival_label": """CASE WHEN flight_status = 'completed'
            THEN ARR_DEL15 IS NOT NULL AND ARR_DEL15 IN (0, 1)
            ELSE ARR_DEL15 IS NULL END""",
        "missing_completed_arrival": """flight_status <> 'completed' OR
            (ARR_TIME IS NOT NULL AND TRIM(ARR_TIME) <> ''
             AND ARR_DELAY IS NOT NULL AND ARR_DEL15 IS NOT NULL)""",
        "invalid_completed_arrival_time": """flight_status <> 'completed' OR
            ARR_TIME RLIKE '^([01][0-9]|2[0-3])[0-5][0-9]$' OR ARR_TIME = '2400'""",
        "nonfinite_completed_delay": """flight_status <> 'completed'
            OR ARR_DELAY IS NULL
            OR (NOT ISNAN(ARR_DELAY) AND ABS(ARR_DELAY) <= 1.7976931348623157E308)""",
    }
    status_counts = ", ".join(
        f"COUNT(CASE WHEN flight_status = '{status}' THEN 1 END) AS {status}_flights"
        for status in ("completed", "cancelled", "diverted")
    )
    rows = [row.asDict() for row in spark.sql(
        "SELECT YEAR, MONTH, COUNT(*) AS total_flights, " + status_counts + ", "
        + violation_counts_sql(rules)
        + " FROM flights_cleaned GROUP BY YEAR, MONTH ORDER BY YEAR, MONTH"
    ).collect()]  # Month summaries only; no flight-level collect/cache.
    total = sum(row["total_flights"] for row in rows)
    violations = {name: sum(row[name] for row in rows) for name in rules}
    checks["input_not_empty"] = total > 0
    checks.update({name: value == 0 for name, value in violations.items()})
    checks.update(reconcile_status_months(rows, baseline))
    result.update(row_count=total, by_month=rows, violations=violations)
    return result


def metric_rules() -> dict[str, str]:
    """Validate signed averages separately from bounded 0-1 rates."""
    rules = {
        "invalid_counts": " AND ".join(
            f"{name} IS NOT NULL AND {name} >= 0" for name in COUNT_COLUMNS
        ),
        "empty_group": "total_flights > 0",
        "status_reconciliation": "total_flights = completed_flights "
                                 "+ cancelled_flights + diverted_flights",
        "label_reconciliation": "completed_flights = delayed_arrival_flights "
                                "+ on_time_arrival_flights",
        "observation_count": "arrival_delay_observed_flights <= completed_flights",
        "reviewed_completed_delay_present": "arrival_delay_observed_flights = completed_flights",
        "delay_sum_null_handling": """CASE WHEN arrival_delay_observed_flights = 0
            THEN arrival_delay_minutes_sum IS NULL
            ELSE arrival_delay_minutes_sum IS NOT NULL
                AND NOT ISNAN(arrival_delay_minutes_sum)
                AND ABS(arrival_delay_minutes_sum) <= 1.7976931348623157E308 END""",
    }
    ratios = {
        **RATE_COMPONENTS,
        "average_arrival_delay": (
            "arrival_delay_minutes_sum", "arrival_delay_observed_flights"
        ),
    }
    for metric, (numerator, denominator) in ratios.items():
        expected = f"CAST({numerator} AS DOUBLE) / NULLIF({denominator}, 0)"
        bounds = f"AND {metric} BETWEEN 0.0 AND 1.0" if metric in RATE_COMPONENTS else ""
        rules[metric + "_formula_or_null"] = f"""
            CASE WHEN {denominator} = 0 THEN {metric} IS NULL
            ELSE {metric} IS NOT NULL AND NOT ISNAN({metric})
                AND ABS({metric}) <= 1.7976931348623157E308
                AND ABS({metric} - ({expected})) <= 1E-12 * GREATEST(1.0, ABS({expected}))
                {bounds} END"""
    return rules


def summarize_table(
    spark: SparkSession, df: DataFrame, view: str, grain: tuple[str, ...]
) -> dict[str, Any]:
    """Collect only month summaries, never the full route/carrier table."""
    expected = {key: "int" if key in ("YEAR", "MONTH") else "string" for key in grain}
    expected.update({name: "bigint" for name in COUNT_COLUMNS})
    expected.update({name: "double" for name in RATE_COMPONENTS})
    expected.update(arrival_delay_minutes_sum="double", average_arrival_delay="double")
    schema = schema_check(df, expected)
    result = {"checks": {"schema": schema["passed"]}, "schema": schema}
    if not schema["passed"]:
        return result
    df.createOrReplaceTempView(view)
    rules = metric_rules()
    rules["null_grain"] = " AND ".join(f"{key} IS NOT NULL" for key in grain)
    sums = ", ".join(f"SUM({name}) AS {name}" for name in COUNT_COLUMNS)
    rows = spark.sql(f"""
        SELECT YEAR, MONTH, COUNT(*) AS aggregate_rows,
            COUNT(*) - COUNT(DISTINCT STRUCT({', '.join(grain)})) AS duplicate_excess,
            {sums}, SUM(arrival_delay_minutes_sum) AS arrival_delay_minutes_sum,
            {violation_counts_sql(rules)}
        FROM {view} GROUP BY YEAR, MONTH ORDER BY YEAR, MONTH
    """).collect()
    violations = {name: sum(row[name] for row in rows) for name in rules}
    violations["duplicate_excess"] = sum(row["duplicate_excess"] for row in rows)
    by_month = [
        {key: row[key] for key in ("YEAR", "MONTH", *COUNT_COLUMNS, "arrival_delay_minutes_sum")}
        for row in rows
    ]
    # Keep failure evidence valid JSON if an aggregate unexpectedly overflows.
    for row in by_month:
        value = row["arrival_delay_minutes_sum"]
        if isinstance(value, float) and not isfinite(value):
            row["arrival_delay_minutes_sum"] = str(value)
    result.update(
        row_count=sum(row["aggregate_rows"] for row in rows),
        by_month=by_month,
        violations=violations,
    )
    result["checks"].update({name: value == 0 for name, value in violations.items()})
    result["checks"]["not_empty"] = bool(rows)
    return result


def reconcile_months(
    actual: list[dict[str, Any]], monthly: list[dict[str, Any]]
) -> dict[str, bool]:
    """Counts match exactly; floating-point signed sums use a small tolerance."""
    actual_map = {(row["YEAR"], row["MONTH"]): row for row in actual}
    expected_map = {(row["YEAR"], row["MONTH"]): row for row in monthly}
    same_periods = actual_map.keys() == expected_map.keys()
    checks = {"same_months": same_periods}
    checks["monthly_counts"] = same_periods and all(
        actual_map[key][name] == expected_map[key][name]
        for key in expected_map for name in COUNT_COLUMNS
    )
    sums_match = same_periods
    for key in actual_map.keys() & expected_map.keys():
        actual_sum = actual_map[key]["arrival_delay_minutes_sum"]
        expected_sum = expected_map[key]["arrival_delay_minutes_sum"]
        if actual_sum is None or expected_sum is None:
            sums_match = sums_match and actual_sum is expected_sum
        else:
            sums_match = sums_match and isclose(actual_sum, expected_sum, rel_tol=1E-12, abs_tol=1E-8)
    checks["monthly_delay_minutes_sum"] = sums_match
    return checks


def reconcile_baseline(monthly: list[dict[str, Any]], baseline: dict[str, Any]) -> dict[str, Any]:
    actual_months = {(r["YEAR"], r["MONTH"]): r["total_flights"] for r in monthly}
    expected_months = {(r["year"], r["month"]): r["row_count"] for r in baseline["by_month"]}
    actual_totals = {name: sum(row[name] for row in monthly) for name in COUNT_COLUMNS}
    checks = {
        "configured_month_coverage": set(actual_months) == set(expected_months)
        == {(EXPECTED_YEAR, month) for month in EXPECTED_MONTHS},
        "phase2_month_counts": actual_months == expected_months,
        "phase2_total": actual_totals["total_flights"] == baseline["total_rows"],
        "phase2_no_invalid_status": baseline["flight_status"]["invalid"] == 0,
    }
    for status in ("completed", "cancelled", "diverted"):
        checks["phase2_" + status] = (
            actual_totals[status + "_flights"] == baseline["flight_status"][status]
        )
    checks.update(reconcile_status_months(monthly, baseline))
    available_sums = [row["arrival_delay_minutes_sum"] for row in monthly
                      if row["arrival_delay_minutes_sum"] is not None]
    annual = dict(actual_totals)
    annual["arrival_delay_minutes_sum"] = sum(available_sums) if available_sums else None
    for metric, (numerator, denominator) in {
        **RATE_COMPONENTS,
        "average_arrival_delay": ("arrival_delay_minutes_sum", "arrival_delay_observed_flights"),
    }.items():
        annual[metric] = (annual[numerator] / annual[denominator]
                          if annual[denominator] > 0 and annual[numerator] is not None else None)
    return {"checks": checks, "actual_totals": actual_totals, "baseline": baseline,
            "annual_summary": annual, "annual_method": "Ratio of sums; no persisted annual table."}


def reconcile_status_months(monthly: list[dict[str, Any]], baseline: dict[str, Any]) -> dict[str, bool]:
    """Exact counts against reviewed Phase 2, including each monthly status."""
    actual = {(r["YEAR"], r["MONTH"]): r for r in monthly}
    expected = {(r["YEAR"], r["MONTH"]): r for r in baseline["status_by_month"]}
    same_periods = (set(actual) == set(expected)
                    == {(EXPECTED_YEAR, month) for month in EXPECTED_MONTHS})
    checks = {"phase2_exact_month_coverage": same_periods and len(monthly) == len(expected)}
    for metric in ("total_flights", "completed_flights", "cancelled_flights", "diverted_flights"):
        checks["phase2_monthly_" + metric] = same_periods and all(
            actual[period][metric] == expected[period][metric] for period in expected
        )
    return checks


def compare_mvp_table(current: DataFrame, reference: DataFrame, grain: tuple[str, ...]) -> dict[str, Any]:
    """Compare Q1 aggregates in Spark, collecting only scalar mismatch counts.

    Counts/keys are exact. Doubles use max(abs_tol, rel_tol * max(abs(a), abs(b))).
    NULL equals only NULL. No rounding, count tolerance, or full-route collect.
    """
    selected = current.filter((F.col("YEAR") == MVP_YEAR) & F.col("MONTH").isin(*MVP_MONTHS))
    expected_types = {f.name: f.dataType.simpleString() for f in current.schema}
    schema = schema_check(reference, expected_types)
    result = {
        "checks": {"schema": schema["passed"]}, "schema": schema,
        "period": {"year": MVP_YEAR, "months": list(MVP_MONTHS)},
        "double_tolerance": {"absolute": REGRESSION_ABS_TOL, "relative": REGRESSION_REL_TOL},
    }
    if not schema["passed"]:
        return result

    def grain_stats(frame: DataFrame) -> dict[str, int]:
        valid_key = F.lit(True)
        for key in grain:
            valid_key = valid_key & F.col(key).isNotNull()
        valid_period = (F.col("YEAR") == MVP_YEAR) & F.col("MONTH").isin(*MVP_MONTHS)
        return frame.agg(
            F.count("*").alias("rows"),
            F.countDistinct(F.struct(*grain)).alias("unique_keys"),
            F.count(F.when(~F.coalesce(valid_key & valid_period, F.lit(False)), 1)).alias("invalid_keys"),
            F.countDistinct(F.struct("YEAR", "MONTH")).alias("periods"),
        ).first().asDict()

    stats = {"current": grain_stats(selected), "mvp": grain_stats(reference)}
    result["grain"] = stats
    result["checks"]["grain_and_periods"] = all(
        s["rows"] > 0 and s["rows"] == s["unique_keys"] and s["invalid_keys"] == 0
        and s["periods"] == len(MVP_MONTHS) for s in stats.values()
    )
    result["checks"]["row_count"] = stats["current"]["rows"] == stats["mvp"]["rows"]
    if not result["checks"]["grain_and_periods"]:
        return result  # Avoid a many-to-many comparison of malformed references.

    joined = (selected.withColumn("_present", F.lit(1)).alias("new")
              .join(reference.withColumn("_present", F.lit(1)).alias("old"), list(grain), "full"))
    differences = {
        "missing_keys": F.col("new._present").isNull(),
        "extra_keys": F.col("old._present").isNull(),
    }
    doubles = (*RATE_COMPONENTS, "arrival_delay_minutes_sum", "average_arrival_delay")
    for metric in COUNT_COLUMNS:
        differences[metric] = ~F.col("new." + metric).eqNullSafe(F.col("old." + metric))
    for metric in doubles:
        left, right = F.col("new." + metric), F.col("old." + metric)
        finite = (~F.isnan(left)) & (~F.isnan(right)) & (
            F.abs(left) <= 1.7976931348623157e308
        ) & (F.abs(right) <= 1.7976931348623157e308)
        tolerance = F.greatest(F.lit(REGRESSION_ABS_TOL),
                              F.lit(REGRESSION_REL_TOL) * F.greatest(F.abs(left), F.abs(right)))
        equal = ((left.isNull() & right.isNull()) | (
            left.isNotNull() & right.isNotNull() & finite & (F.abs(left - right) <= tolerance)
        ))
        differences[metric] = ~F.coalesce(equal, F.lit(False))
    counts = joined.agg(*[
        F.count(F.when(condition, 1)).alias(name) for name, condition in differences.items()
    ]).first().asDict()
    result["mismatches"] = {
        "keys": {k: counts[k] for k in ("missing_keys", "extra_keys")},
        "exact_counts": {k: counts[k] for k in COUNT_COLUMNS},
        "double_metrics": {k: counts[k] for k in doubles},
    }
    for group, metrics in result["mismatches"].items():
        result["checks"][group] = all(value == 0 for value in metrics.values())
    return result


def require_checks(result: dict[str, Any], label: str) -> None:
    failed = [name for name, passed in result["checks"].items() if not passed]
    result["failed_checks"] = failed
    result["status"] = "FAIL" if failed else "PASS"
    if failed:
        raise ValueError(f"{label}: {', '.join(failed)}")
