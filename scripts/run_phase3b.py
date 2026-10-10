"""Run three supplementary SQL analyses; write only new Phase 3B CSV artifacts."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import datetime, timezone
from math import isclose, isfinite
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import SparkSession


ROOT = Path(__file__).resolve().parents[1]
CAUSES = ("CARRIER_DELAY", "WEATHER_DELAY", "NAS_DELAY", "SECURITY_DELAY", "LATE_AIRCRAFT_DELAY")
REQUIRED_TYPES = {
    "YEAR": "int", "MONTH": "int", "DAY_OF_WEEK": "int",
    "CRS_DEP_TIME": "string", "flight_status": "string", "ARR_DEL15": "int",
    **{column: "double" for column in CAUSES},
}
QUERIES = {
    "day_of_week": "008_day_of_week.sql",
    "departure_time_band": "009_departure_time_band.sql",
    "delay_causes": "010_delay_causes.sql",
}
FLIGHT_COUNTS = ("total_flights", "completed_flights", "delayed_arrival_flights")
ELIGIBLE = "flight_status = 'completed' AND ARR_DEL15 = 1"
HHMM = "^([01][0-9]|2[0-3])[0-5][0-9]$"
BAND_LIMITS = {
    "Night": ("0000", "0559"), "Morning": ("0600", "1159"),
    "Afternoon": ("1200", "1759"), "Evening": ("1800", "2359"),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", type=Path, default=ROOT / "data/processed/flights_cleaned")
    parser.add_argument("--output-dir", type=Path, default=ROOT / "artifacts/phase3b")
    return parser.parse_args()


def check(report: dict, name: str, passed: bool, details=None) -> None:
    report["checks"].append({"name": name, "passed": bool(passed), "details": details})
    if not passed:
        raise ValueError(f"Validation failed: {name}")


def same_number(actual, expected) -> bool:
    if actual is None or expected is None:
        return actual is expected
    return isfinite(actual) and isfinite(expected) and isclose(
        actual, expected, rel_tol=1E-12, abs_tol=1E-9
    )


def valid_ratio(value, numerator, denominator) -> bool:
    if numerator is None or denominator is None or denominator == 0:
        return value is None
    return value is not None and 0 <= value <= 1 and same_number(value, numerator / denominator)


def collect_source_months(spark: SparkSession, report: dict) -> dict:
    """Independent monthly counts/minutes used to reconcile the three outputs."""
    any_reported = " OR ".join(f"{c} IS NOT NULL" for c in CAUSES)
    all_reported = " AND ".join(f"{c} IS NOT NULL" for c in CAUSES)
    bad_cause = " OR ".join(
        f"({c} IS NOT NULL AND ({c} < 0 OR ISNAN({c}) OR ABS({c}) > 1.7976931348623157E308))"
        for c in CAUSES
    )
    cause_metrics = ",\n".join(
        f"COUNT(CASE WHEN {ELIGIBLE} THEN {c} END) AS reported_{c}, "
        f"SUM(CASE WHEN {ELIGIBLE} THEN {c} END) AS minutes_{c}"
        for c in CAUSES
    )
    band_metrics = []
    for band, (start, end) in BAND_LIMITS.items():
        condition = f"(CRS_DEP_TIME RLIKE '{HHMM}' AND CRS_DEP_TIME BETWEEN '{start}' AND '{end}')"
        if band == "Night":
            condition += " OR CRS_DEP_TIME = '2400'"
        band_metrics.append(f"COUNT(CASE WHEN {condition} THEN 1 END) AS band_{band}")
    rows = [row.asDict() for row in spark.sql(f"""
        SELECT YEAR, MONTH, COUNT(*) AS total_flights,
            COUNT(CASE WHEN flight_status = 'completed' THEN 1 END) AS completed_flights,
            COUNT(CASE WHEN {ELIGIBLE} THEN 1 END) AS delayed_arrival_flights,
            COUNT(CASE WHEN NOT COALESCE(DAY_OF_WEEK BETWEEN 1 AND 7, FALSE)
                       THEN 1 END) AS invalid_weekdays,
            COUNT(CASE WHEN NOT COALESCE(flight_status IN ('completed','cancelled','diverted'), FALSE)
                       THEN 1 END) AS invalid_statuses,
            COUNT(CASE WHEN NOT COALESCE(CASE WHEN flight_status = 'completed'
                       THEN ARR_DEL15 IN (0,1) ELSE ARR_DEL15 IS NULL END, FALSE)
                       THEN 1 END) AS invalid_labels,
            COUNT(CASE WHEN CRS_DEP_TIME IS NULL THEN 1 END) AS missing_times,
            COUNT(CASE WHEN CRS_DEP_TIME IS NOT NULL AND CRS_DEP_TIME <> '2400'
                       AND NOT (CRS_DEP_TIME RLIKE '{HHMM}') THEN 1 END) AS invalid_times,
            COUNT(CASE WHEN CRS_DEP_TIME = '2400' THEN 1 END) AS midnight_2400,
            COUNT(CASE WHEN {bad_cause} THEN 1 END) AS invalid_cause_rows,
            COUNT(CASE WHEN {ELIGIBLE} AND ({any_reported}) THEN 1 END) AS any_reported,
            COUNT(CASE WHEN {ELIGIBLE} AND ({all_reported}) THEN 1 END) AS all_reported,
            {cause_metrics}, {', '.join(band_metrics)}
        FROM flights_cleaned GROUP BY YEAR, MONTH ORDER BY YEAR, MONTH
    """).collect()]
    check(report, "input_nonempty", sum(r["total_flights"] for r in rows) > 0)
    check(report, "input_mvp_months", {(r["YEAR"], r["MONTH"]) for r in rows} == {(2025, m) for m in (1, 2, 3)})
    for field in ("invalid_weekdays", "invalid_statuses", "invalid_labels", "invalid_cause_rows"):
        count = sum(r[field] for r in rows)
        check(report, "input_" + field, count == 0, {"violating_rows": count})
    check(report, "finite_monthly_cause_sums", all(
        r["minutes_" + c] is None or isfinite(r["minutes_" + c]) for r in rows for c in CAUSES
    ))
    report["input"]["row_count"] = sum(r["total_flights"] for r in rows)
    report["input"]["by_month"] = rows
    return {(r["YEAR"], r["MONTH"]): r for r in rows}


def validate_performance(name: str, rows: list[dict], source: dict, report: dict) -> None:
    dimension = "DAY_OF_WEEK" if name == "day_of_week" else "time_band"
    domain = set(range(1, 8)) if name == "day_of_week" else {
        "Night", "Morning", "Afternoon", "Evening", "Missing", "Invalid"
    }
    keys = [(r["YEAR"], r["MONTH"], r[dimension]) for r in rows]
    check(report, name + ".grain_and_domain", len(keys) == len(set(keys)) and all(
        (y, m) in source and group in domain for y, m, group in keys
    ))
    check(report, name + ".counts_and_rates", all(
        0 <= r["delayed_arrival_flights"] <= r["completed_flights"] <= r["total_flights"]
        and r["total_flights"] > 0
        and valid_ratio(r["arrival_delay_rate"], r["delayed_arrival_flights"], r["completed_flights"])
        for r in rows
    ))
    check(report, name + ".monthly_counts", all(
        sum(r[c] for r in rows if (r["YEAR"], r["MONTH"]) == period) == expected[c]
        for period, expected in source.items() for c in FLIGHT_COUNTS
    ))
    if name == "departure_time_band":
        check(report, "departure_time_band.hour_boundaries", all(
            sum(r["total_flights"] for r in rows
                if (r["YEAR"], r["MONTH"]) == period and r["time_band"] == band) == expected["band_" + band]
            for period, expected in source.items() for band in BAND_LIMITS
        ))
        check(report, "departure_time_band.missing_invalid_retained", all(
            sum(r["total_flights"] for r in rows
                if (r["YEAR"], r["MONTH"]) == period and r["time_band"] == band) == expected[field]
            for period, expected in source.items()
            for band, field in (("Missing", "missing_times"), ("Invalid", "invalid_times"))
        ))
        check(report, "departure_time_band.2400_is_night", all(
            0 <= r["scheduled_2400_flights"] <= r["total_flights"]
            and (r["time_band"] == "Night" or r["scheduled_2400_flights"] == 0) for r in rows
        ) and all(
            sum(r["scheduled_2400_flights"] for r in rows
                if (r["YEAR"], r["MONTH"]) == period) == expected["midnight_2400"]
            for period, expected in source.items()
        ))


def validate_causes(rows: list[dict], source: dict, report: dict) -> None:
    expected_keys = {(y, m, c) for (y, m), s in source.items()
                     if s["delayed_arrival_flights"] > 0 for c in CAUSES}
    keys = [(r["YEAR"], r["MONTH"], r["cause_category"]) for r in rows]
    check(report, "delay_causes.grain_and_categories", len(keys) == len(set(keys)) and set(keys) == expected_keys)
    coverage_ok = minutes_ok = shares_ok = True
    for row in rows:
        original = source[(row["YEAR"], row["MONTH"])]
        category = row["cause_category"]
        available = [original["minutes_" + c] for c in CAUSES if original["minutes_" + c] is not None]
        denominator = sum(available) if available else None
        coverage_ok = coverage_ok and (
            row["eligible_delayed_flights"] == original["delayed_arrival_flights"]
            and row["reported_cause_flights"] == original["reported_" + category]
            and row["flights_with_any_reported_cause"] == original["any_reported"]
            and row["flights_with_all_reported_causes"] == original["all_reported"]
        )
        minutes_ok = minutes_ok and same_number(row["attributed_minutes"], original["minutes_" + category])
        shares_ok = shares_ok and same_number(row["monthly_attributed_minutes"], denominator)
        shares_ok = shares_ok and valid_ratio(row["cause_minute_share"], row["attributed_minutes"], denominator)
    check(report, "delay_causes.coverage_counts", coverage_ok)
    check(report, "delay_causes.reported_minutes", minutes_ok)
    check(report, "delay_causes.denominators_and_shares", shares_ok)
    for period in source:
        group = [r for r in rows if (r["YEAR"], r["MONTH"]) == period]
        if not group:
            continue  # No eligible delayed flights; no cause rows are manufactured.
        denominator = group[0]["monthly_attributed_minutes"]
        shares = [r["cause_minute_share"] for r in group if r["cause_minute_share"] is not None]
        reconciles = same_number(sum(shares), 1.0) if denominator and denominator > 0 else not shares
        check(report, f"delay_causes.share_sum_{period[0]}_{period[1]:02d}", reconciles)


def export_csv(spark: SparkSession, df, path: Path, rows: list[dict], report: dict, name: str) -> int:
    """CSV has no stored types: parse with the SQL result schema and check values."""
    df.coalesce(1).write.mode("errorifexists").option("header", True).option("nullValue", "NULL").csv(path.as_posix())
    loaded = (spark.read.schema(df.schema).option("header", True)
              .option("enforceSchema", False).option("mode", "FAILFAST")
              .option("nullValue", "NULL").csv(path.as_posix()))
    restored = loaded.collect()
    check(report, name + ".csv_row_count", len(restored) == len(rows))
    expected = Counter(tuple(r[c] for c in df.columns) for r in rows)
    check(report, name + ".csv_values", Counter(tuple(r) for r in restored) == expected)
    return len(restored)


def main() -> None:
    args = parse_args()
    input_dir, output_dir = args.input_dir.resolve(), args.output_dir.resolve()
    if not input_dir.is_dir():
        raise FileNotFoundError(f"Cleaned Parquet not found: {input_dir}")
    if not output_dir.is_relative_to(ROOT / "artifacts") or output_dir == ROOT / "artifacts":
        raise ValueError("Choose a new output directory beneath this project's artifacts/.")
    if output_dir.is_relative_to(input_dir) or input_dir.is_relative_to(output_dir):
        raise ValueError("Input and output must not overlap.")
    output_dir.mkdir(parents=True, exist_ok=False)  # Reserve only a new directory.
    report = {
        "phase": "3B", "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "overall_status": "RUNNING", "failure_stage": None,
        "input": {"path": input_dir.as_posix(), "status": "NOT_RUN"},
        "output_directory": output_dir.as_posix(), "csv_null_value": "NULL",
        "analyses": {name: {"status": "NOT_RUN"} for name in QUERIES},
        "checks": [], "errors": [],
    }
    spark = cached = None
    stage, active = "input", None
    try:
        report["input"]["status"] = "RUNNING"
        spark = (SparkSession.builder.appName("airline-phase3b")
                 .config("spark.sql.shuffle.partitions", "8")
                 .config("spark.sql.session.timeZone", "UTC")
                 .config("spark.sql.ansi.enabled", "true").getOrCreate())
        spark.sparkContext.setLogLevel("WARN")
        report["runtime"] = {"spark_version": spark.version, "master": spark.sparkContext.master}
        frame = spark.read.parquet(input_dir.as_posix())
        actual = {field.name: field.dataType.simpleString() for field in frame.schema}
        check(report, "required_input_schema", all(actual.get(c) == t for c, t in REQUIRED_TYPES.items()),
              {"expected": REQUIRED_TYPES, "actual_required": {c: actual.get(c) for c in REQUIRED_TYPES}})
        cached = frame.select(*REQUIRED_TYPES).persist(StorageLevel.MEMORY_AND_DISK)
        cached.createOrReplaceTempView("flights_cleaned")
        source = collect_source_months(spark, report)
        report["input"]["status"] = "PASS"
        for name, filename in QUERIES.items():
            active, stage = name, name + ":validation"
            result = report["analyses"][name]
            result.update(status="RUNNING", sql_file=filename)
            df = spark.sql((ROOT / "sql/analytics" / filename).read_text(encoding="utf-8"))
            rows = [row.asDict() for row in df.collect()]  # At most 21/18/15 aggregate rows.
            if name == "delay_causes":
                validate_causes(rows, source, report)
            else:
                validate_performance(name, rows, source, report)
            stage = name + ":csv_export_read_back"
            path = output_dir / name
            result.update(row_count=len(rows), schema=df.schema.simpleString(), csv_path=path.as_posix())
            result["read_back_rows"] = export_csv(spark, df, path, rows, report, name)
            result["status"] = "PASS"
            print(f"\n{name}: {len(rows)} rows; validation and CSV read-back PASS")
            for row in rows[:10]:
                print(row)
        report["overall_status"] = "PASS"
    except Exception as error:
        report.update(overall_status="FAIL", failure_stage=stage)
        (report["input"] if active is None else report["analyses"][active])["status"] = "FAIL"
        report["errors"].append({"type": type(error).__name__, "message": str(error)})
        raise
    finally:
        try:
            report["failed_checks"] = [c["name"] for c in report["checks"] if not c["passed"]]
            with (output_dir / "validation_summary.json").open("x", encoding="utf-8") as handle:
                json.dump(report, handle, indent=2, ensure_ascii=False, allow_nan=False)
            print(f"\nPhase 3B: {report['overall_status']}; evidence: {output_dir}")
        finally:
            if cached is not None:
                cached.unpersist()
            if spark is not None:
                spark.stop()


if __name__ == "__main__":
    main()
