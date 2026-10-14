"""Run three supplementary SQL analyses; write only new Phase 3B CSV artifacts."""

from __future__ import annotations

import argparse
import hashlib
import json
from collections import Counter
from datetime import datetime, timezone
from math import isclose, isfinite
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import SparkSession

from airline_analytics.cleaning import expected_cleaned_types
from airline_analytics.analytics_validation import (
    TABLE_GRAINS, reconcile_months, require_checks, summarize_table,
)
from airline_analytics.schema import EXPECTED_MONTHS, EXPECTED_YEAR


ROOT = Path(__file__).resolve().parents[1]
CAUSES = ("CARRIER_DELAY", "WEATHER_DELAY", "NAS_DELAY", "SECURITY_DELAY", "LATE_AIRCRAFT_DELAY")
REQUIRED_TYPES = {
    "YEAR": "int", "MONTH": "int", "DAY_OF_WEEK": "int",
    "CRS_DEP_TIME": "string", "flight_status": "string", "ARR_DEL15": "int",
    "ARR_TIME": "string", "ARR_DELAY": "double", "CANCELLED": "int", "DIVERTED": "int",
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
MVP_MONTHS = (1, 2, 3)  # Historical regression window, not an input filter.
REL_TOL, ABS_TOL = 1E-12, 1E-9
DIMENSIONS = {
    "day_of_week": ("DAY_OF_WEEK", tuple(range(1, 8))),
    "departure_time_band": ("time_band", (*BAND_LIMITS, "Missing", "Invalid")),
    "delay_causes": ("cause_category", CAUSES),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", type=Path, default=ROOT / "data/processed/flights_cleaned_2025_reviewed")
    parser.add_argument("--phase2-report", type=Path, default=ROOT / "artifacts/phase2_2025_reviewed/validation_summary.json")
    parser.add_argument("--phase3a-report", type=Path, default=ROOT / "artifacts/phase3_2025_reviewed/validation_summary.json")
    parser.add_argument("--monthly-input", type=Path, default=ROOT / "data/analytics/2025_reviewed/monthly_performance")
    parser.add_argument("--mvp-dir", type=Path, default=ROOT / "artifacts/phase3b")
    parser.add_argument("--output-dir", type=Path, default=ROOT / "artifacts/phase3b_2025_reviewed")
    return parser.parse_args()


def check(report: dict, name: str, passed: bool, details=None) -> None:
    report["checks"].append({"name": name, "passed": bool(passed), "details": details})
    if not passed:
        raise ValueError(f"Validation failed: {name}")


def same_number(actual, expected) -> bool:
    if actual is None or expected is None:
        return actual is expected
    return isfinite(actual) and isfinite(expected) and isclose(
        actual, expected, rel_tol=REL_TOL, abs_tol=ABS_TOL
    )


def valid_ratio(value, numerator, denominator) -> bool:
    if numerator is None or denominator is None or denominator == 0:
        return value is None
    return value is not None and 0 <= value <= 1 and same_number(value, numerator / denominator)


def project_path(value: str | Path) -> Path:
    path = Path(value)
    return (path if path.is_absolute() else ROOT / path).resolve()


def evidence_checks_ok(value) -> bool:
    """Inspect existing nested check evidence; do not execute prior phases."""
    if isinstance(value, list):
        return all(evidence_checks_ok(item) for item in value)
    if not isinstance(value, dict):
        return True
    if (value.get("errors") or value.get("failed_checks")
            or value.get("status") in ("FAIL", "NOT_RUN", "RUNNING")
            or value.get("passed") is False or value.get("pass") is False):
        return False
    if "checks" in value:
        checks = value["checks"]
        if not checks or (isinstance(checks, dict) and not all(v is True for v in checks.values())):
            return False
    return all(evidence_checks_ok(item) for item in value.values())


def load_evidence(args: argparse.Namespace, report: dict) -> tuple[dict, dict]:
    """Bind both PASS reports to this cleaned path and the exact Phase 2 JSON."""
    p2_bytes = args.phase2_report.read_bytes()
    p3_bytes = args.phase3a_report.read_bytes()
    p2, p3 = json.loads(p2_bytes.decode("utf-8-sig")), json.loads(p3_bytes.decode("utf-8-sig"))
    check(report, "phase2_evidence_pass", p2.get("phase") == 2
          and p2.get("overall_status") == "PASS" and evidence_checks_ok(p2)
          and all(p2[s]["status"] == "PASS" for s in ("pre_write", "write", "read_back")))
    check(report, "phase3a_evidence_pass", p3.get("phase") == "3A"
          and p3.get("overall_status") == "PASS" and evidence_checks_ok(p3)
          and all(p3[s]["status"] == "PASS" for s in (
              "preflight", "input", "pre_write", "write", "read_back", "mvp_regression")))
    check(report, "phase3a_all_four_tables_verified", all(
        set(p3[stage]["tables"]) == set(TABLE_GRAINS)
        and all(p3[stage]["tables"][name]["status"] == "PASS" for name in TABLE_GRAINS)
        for stage in ("pre_write", "read_back", "mvp_regression")
    ))
    snapshot = p3["preflight"]["phase2_baseline"]
    p2_hash = hashlib.sha256(p2_bytes).hexdigest()
    check(report, "same_cleaned_path", args.input_dir.resolve()
          == project_path(p2["output"]["path"]) == project_path(p3["input_path"])
          == project_path(snapshot["provenance"]["cleaned_path"]))
    check(report, "phase3a_uses_same_phase2_evidence", args.phase2_report.resolve()
          == project_path(p3["phase2_report_path"])
          and snapshot["provenance"]["sha256"] == p2_hash)
    check(report, "monthly_path_matches_phase3a", args.monthly_input.resolve()
          == project_path(p3["output"]["directory"]) / "monthly_performance")
    check(report, "phase2_snapshot_unchanged", all(
        snapshot[k] == p2["read_back"]["summary"][k] == p2["pre_write"]["cleaned"][k]
        for k in ("total_rows", "by_month", "flight_status")
    ))
    baseline = {(r["YEAR"], r["MONTH"]): r for r in snapshot["status_by_month"]}
    check(report, "baseline_month_coverage", len(baseline) == len(snapshot["status_by_month"])
          and set(baseline) == {(EXPECTED_YEAR, m) for m in EXPECTED_MONTHS})
    check(report, "phase2_baseline_totals", snapshot["flight_status"]["invalid"] == 0
          and sum(r["total_flights"] for r in baseline.values()) == snapshot["total_rows"]
          and all(sum(r[s + "_flights"] for r in baseline.values()) == snapshot["flight_status"][s]
                  for s in ("completed", "cancelled", "diverted"))
          and {(r["year"], r["month"]): r["row_count"] for r in snapshot["by_month"]}
          == {p: r["total_flights"] for p, r in baseline.items()})
    partitions = p2["pre_write"]["partition_counts"]["cleaned"]
    check(report, "baseline_monthly_status_matches_phase2", all(
        row[status + "_flights"] == sum(p["count"] for p in partitions
            if (p["year"], p["month"]) == period and p["flight_status"] == status)
        for period, row in baseline.items() for status in ("completed", "cancelled", "diverted")
    ))
    report["evidence"] = {
        "phase2": {"path": args.phase2_report.resolve().as_posix(), "sha256": p2_hash},
        "phase3a": {"path": args.phase3a_report.resolve().as_posix(),
                    "sha256": hashlib.sha256(p3_bytes).hexdigest()},
        "cleaned_path": args.input_dir.resolve().as_posix(),
        "monthly_path": args.monthly_input.resolve().as_posix(),
        "phase2_status_by_month": list(baseline.values()),
    }
    return baseline, p3["read_back"]["tables"]["monthly_performance"]


def validate_monthly_baseline(spark: SparkSession, args, recorded: dict, report: dict) -> dict:
    """Read only the small monthly aggregate and recheck its schema/KPI contract."""
    frame = spark.read.parquet(args.monthly_input.resolve().as_posix())
    summary = summarize_table(spark, frame, "phase3b_monthly_baseline", ("YEAR", "MONTH"))
    report["monthly_baseline"] = summary
    require_checks(summary, "Phase 3A monthly Parquet")
    comparisons = reconcile_months(summary["by_month"], recorded["by_month"])
    comparisons["row_count"] = summary["row_count"] == recorded["row_count"]
    check(report, "monthly_parquet_matches_phase3a_evidence", all(comparisons.values()), comparisons)
    return {(r["YEAR"], r["MONTH"]): r for r in summary["by_month"]}


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
    weekday_metrics = ", ".join(
        f"COUNT(CASE WHEN DAY_OF_WEEK = {day} THEN 1 END) AS weekday_{day}" for day in range(1, 8)
    )
    rows = [row.asDict() for row in spark.sql(f"""
        SELECT YEAR, MONTH, COUNT(*) AS total_flights,
            COUNT(CASE WHEN flight_status = 'completed' THEN 1 END) AS completed_flights,
            COUNT(CASE WHEN flight_status = 'cancelled' THEN 1 END) AS cancelled_flights,
            COUNT(CASE WHEN flight_status = 'diverted' THEN 1 END) AS diverted_flights,
            COUNT(CASE WHEN flight_status = 'completed' AND ARR_DEL15 = 0
                       THEN 1 END) AS on_time_arrival_flights,
            COUNT(CASE WHEN {ELIGIBLE} THEN 1 END) AS delayed_arrival_flights,
            COUNT(CASE WHEN NOT COALESCE(DAY_OF_WEEK BETWEEN 1 AND 7, FALSE)
                       THEN 1 END) AS invalid_weekdays,
            COUNT(CASE WHEN NOT COALESCE(flight_status IN ('completed','cancelled','diverted'), FALSE)
                       THEN 1 END) AS invalid_statuses,
            COUNT(CASE WHEN NOT (flight_status <=> CASE
                       WHEN CANCELLED = 0 AND DIVERTED = 0 THEN 'completed'
                       WHEN CANCELLED = 1 AND DIVERTED = 0 THEN 'cancelled'
                       WHEN CANCELLED = 0 AND DIVERTED = 1 THEN 'diverted'
                       ELSE 'invalid' END) THEN 1 END) AS status_flag_mismatches,
            COUNT(CASE WHEN flight_status = 'completed' AND
                       (ARR_TIME IS NULL OR TRIM(ARR_TIME) = '' OR ARR_DELAY IS NULL
                        OR ARR_DEL15 IS NULL) THEN 1 END) AS missing_completed_arrival,
            COUNT(CASE WHEN ARR_DELAY IS NOT NULL AND
                       (ISNAN(ARR_DELAY) OR ABS(ARR_DELAY) > 1.7976931348623157E308)
                       THEN 1 END) AS nonfinite_arrival_delay,
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
            {cause_metrics}, {', '.join(band_metrics)}, {weekday_metrics}
        FROM flights_cleaned GROUP BY YEAR, MONTH ORDER BY YEAR, MONTH
    """).limit(len(EXPECTED_MONTHS) + 1).collect()]
    check(report, "input_nonempty", sum(r["total_flights"] for r in rows) > 0)
    check(report, "input_configured_months", {(r["YEAR"], r["MONTH"]) for r in rows}
          == {(EXPECTED_YEAR, month) for month in EXPECTED_MONTHS})
    for field in ("invalid_weekdays", "invalid_statuses", "invalid_labels", "invalid_cause_rows",
                  "status_flag_mismatches", "missing_completed_arrival", "nonfinite_arrival_delay"):
        count = sum(r[field] for r in rows)
        check(report, "input_" + field, count == 0, {"violating_rows": count})
    check(report, "finite_monthly_cause_sums", all(
        r["minutes_" + c] is None or isfinite(r["minutes_" + c]) for r in rows for c in CAUSES
    ))
    report["input"]["row_count"] = sum(r["total_flights"] for r in rows)
    report["input"]["by_month"] = rows
    return {(r["YEAR"], r["MONTH"]): r for r in rows}


def reconcile_source(source: dict, phase2: dict, monthly: dict, report: dict) -> None:
    check(report, "source_baseline_periods", source.keys() == phase2.keys() == monthly.keys())
    status_metrics = ("total_flights", "completed_flights", "cancelled_flights", "diverted_flights")
    for metric in status_metrics:
        check(report, "phase2_monthly_" + metric, all(
            source[p][metric] == phase2[p][metric] == monthly[p][metric] for p in source
        ))
    for metric in ("delayed_arrival_flights", "on_time_arrival_flights"):
        check(report, "phase3a_monthly_" + metric, all(
            source[p][metric] == monthly[p][metric] for p in source
        ))
    check(report, "source_status_and_label_reconciliation", all(
        r["total_flights"] == r["completed_flights"] + r["cancelled_flights"] + r["diverted_flights"]
        and r["completed_flights"] == r["delayed_arrival_flights"] + r["on_time_arrival_flights"]
        for r in source.values()
    ))
    report["input"]["annual_counts"] = {
        metric: sum(r[metric] for r in source.values())
        for metric in (*status_metrics, "delayed_arrival_flights", "on_time_arrival_flights")
    }


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
        all(r[c] is not None for c in FLIGHT_COUNTS)
        and 0 <= r["delayed_arrival_flights"] <= r["completed_flights"] <= r["total_flights"]
        and r["total_flights"] > 0
        and valid_ratio(r["arrival_delay_rate"], r["delayed_arrival_flights"], r["completed_flights"])
        for r in rows
    ))
    if name == "day_of_week":
        expected_keys = {(y, m, day) for (y, m), r in source.items()
                         for day in domain if r[f"weekday_{day}"] > 0}
        check(report, "day_of_week.observed_grain_coverage", set(keys) == expected_keys and all(
            r["total_flights"] == source[(r["YEAR"], r["MONTH"])][f"weekday_{r['DAY_OF_WEEK']}"]
            for r in rows
        ))
    check(report, name + ".monthly_counts", all(
        sum(r[c] for r in rows if (r["YEAR"], r["MONTH"]) == period) == expected[c]
        for period, expected in source.items() for c in FLIGHT_COUNTS
    ))
    if name == "departure_time_band":
        expected_keys = {(y, m, band) for (y, m), r in source.items()
                         for band, field in (
                             *[(b, "band_" + b) for b in BAND_LIMITS],
                             ("Missing", "missing_times"), ("Invalid", "invalid_times")) if r[field] > 0}
        check(report, "departure_time_band.observed_grain_coverage", set(keys) == expected_keys)
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
    check(report, "delay_causes.nonnegative_counts_and_null_minutes", all(
        all(r[c] is not None and r[c] >= 0 for c in (
            "eligible_delayed_flights", "reported_cause_flights", "flights_with_any_reported_cause",
            "flights_with_all_reported_causes"))
        and r["reported_cause_flights"] <= r["eligible_delayed_flights"]
        and 0 <= r["flights_with_all_reported_causes"] <= r["flights_with_any_reported_cause"]
        <= r["eligible_delayed_flights"]
        and ((r["reported_cause_flights"] == 0 and r["attributed_minutes"] is None)
             or (r["reported_cause_flights"] > 0 and r["attributed_minutes"] is not None
                 and isfinite(r["attributed_minutes"]) and r["attributed_minutes"] >= 0))
        for r in rows
    ))
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


def read_csv(spark: SparkSession, schema, path: Path):
    return (spark.read.schema(schema).option("header", True)
            .option("enforceSchema", False).option("mode", "FAILFAST")
            .option("nullValue", "NULL").csv(path.as_posix()))


def bounded_rows(df, name: str, months, report: dict, label: str) -> list[dict]:
    # Safety ceiling from allowed grain values, NOT an expected output row count.
    ceiling = len(months) * len(DIMENSIONS[name][1])
    rows = [row.asDict() for row in df.limit(ceiling + 1).collect()]
    check(report, label + ".bounded_rows", len(rows) <= ceiling)
    return rows


def load_mvp_evidence(args, report: dict) -> dict:
    path = args.mvp_dir / "validation_summary.json"
    content = path.read_bytes()
    evidence = json.loads(content.decode("utf-8-sig"))
    check(report, "mvp_evidence_pass", evidence.get("phase") == "3B"
          and evidence.get("overall_status") == "PASS" and evidence_checks_ok(evidence)
          and evidence.get("csv_null_value") == "NULL"
          and set(evidence.get("analyses", {})) == set(QUERIES)
          and all(evidence["analyses"][name]["status"] == "PASS" for name in QUERIES))
    check(report, "mvp_csv_paths", all(
        project_path(evidence["analyses"][name]["csv_path"]) == args.mvp_dir / name
        and any((args.mvp_dir / name).glob("*.csv")) for name in QUERIES
    ))
    report["evidence"]["mvp_phase3b"] = {
        "path": path.as_posix(), "sha256": hashlib.sha256(content).hexdigest(),
    }
    return evidence


def validate_mvp(spark: SparkSession, name: str, schema, current: list[dict],
                 args, evidence: dict, report: dict) -> None:
    """Compare only small CSV aggregates; NULL never equals zero, counts are exact."""
    result = {
        "status": "FAIL", "reference_path": (args.mvp_dir / name).as_posix(),
        "period": {"year": EXPECTED_YEAR, "months": list(MVP_MONTHS)},
        "double_tolerance": {"absolute": ABS_TOL, "relative": REL_TOL},
    }
    report["mvp_regression"][name] = result
    prefix = name + ".mvp"
    recorded = evidence["analyses"][name]
    check(report, prefix + ".schema", schema.simpleString() == recorded["schema"])
    reference = bounded_rows(read_csv(spark, schema, args.mvp_dir / name), name,
                             MVP_MONTHS, report, prefix)
    selected = [r for r in current if r["YEAR"] == EXPECTED_YEAR and r["MONTH"] in MVP_MONTHS]
    dimension, domain = DIMENSIONS[name]
    keys = ("YEAR", "MONTH", dimension)
    old = {tuple(r[k] for k in keys): r for r in reference}
    new = {tuple(r[k] for k in keys): r for r in selected}
    result.update(reference_rows=len(reference), current_rows=len(selected))
    check(report, prefix + ".grain_and_periods",
          len(old) == len(reference) and len(new) == len(selected)
          and all(y == EXPECTED_YEAR and m in MVP_MONTHS and group in domain
                  for y, m, group in (*old, *new)))
    check(report, prefix + ".reference_evidence_count",
          len(reference) == recorded["row_count"] == recorded["read_back_rows"])
    common = old.keys() & new.keys()
    counts = [f.name for f in schema if f.name not in keys and f.dataType.simpleString() in ("int", "bigint")]
    doubles = [f.name for f in schema if f.dataType.simpleString() == "double"]
    mismatches = {
        "keys": {"missing_keys": len(old.keys() - new.keys()), "extra_keys": len(new.keys() - old.keys())},
        "exact_counts": {c: sum(new[k][c] != old[k][c] for k in common) for c in counts},
        "double_metrics": {c: sum(not same_number(new[k][c], old[k][c]) for k in common) for c in doubles},
    }
    result["mismatches"] = mismatches
    for group, values in mismatches.items():
        check(report, prefix + "." + group, all(v == 0 for v in values.values()), values)
    result["status"] = "PASS"


def export_csv(spark: SparkSession, df, path: Path, rows: list[dict], report: dict, name: str) -> list[dict]:
    """CSV has no stored types: parse with the SQL result schema and check values."""
    df.coalesce(1).write.mode("errorifexists").option("header", True).option("nullValue", "NULL").csv(path.as_posix())
    loaded = read_csv(spark, df.schema, path)
    check(report, name + ".csv_schema", loaded.schema.simpleString() == df.schema.simpleString())
    restored = bounded_rows(loaded, name, EXPECTED_MONTHS, report, name + ".csv")
    check(report, name + ".csv_row_count", len(restored) == len(rows))
    expected = Counter(tuple(r[c] for c in df.columns) for r in rows)
    check(report, name + ".csv_values", Counter(tuple(r[c] for c in df.columns) for r in restored) == expected)
    return restored


def main() -> None:
    args = parse_args()
    for option in ("input_dir", "phase2_report", "phase3a_report", "monthly_input", "mvp_dir", "output_dir"):
        setattr(args, option, project_path(getattr(args, option)))
    input_dir, output_dir = args.input_dir.resolve(), args.output_dir.resolve()
    if not input_dir.is_dir():
        raise FileNotFoundError(f"Cleaned Parquet not found: {input_dir}")
    if not output_dir.is_relative_to(ROOT / "artifacts") or output_dir == ROOT / "artifacts":
        raise ValueError("Choose a new output directory beneath this project's artifacts/.")
    protected = (input_dir, args.phase2_report.parent, args.phase3a_report.parent,
                 args.monthly_input.parent, args.mvp_dir, ROOT / "artifacts/phase3b")
    if any(output_dir.is_relative_to(path) or path.is_relative_to(output_dir) for path in protected):
        raise ValueError("Output must not overlap input, prior evidence, or MVP directories.")
    output_dir.mkdir(parents=True, exist_ok=False)  # Reserve only a new directory.
    report = {
        "phase": "3B", "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "overall_status": "RUNNING", "failure_stage": None,
        "input": {"path": input_dir.as_posix(), "status": "NOT_RUN"},
        "output_directory": output_dir.as_posix(), "csv_null_value": "NULL",
        "analyses": {name: {"status": "NOT_RUN"} for name in QUERIES},
        "preflight": {"status": "RUNNING"}, "mvp_regression": {},
        "checks": [], "errors": [],
    }
    spark = cached = None
    stage, active = "preflight", None
    try:
        phase2, recorded_monthly = load_evidence(args, report)
        mvp_evidence = load_mvp_evidence(args, report)
        report["preflight"]["status"] = "PASS"
        stage = "input"
        report["input"]["status"] = "RUNNING"
        spark = (SparkSession.builder.appName("airline-phase3b")
                 .config("spark.sql.shuffle.partitions", "8")
                 .config("spark.sql.session.timeZone", "UTC")
                 .config("spark.sql.ansi.enabled", "true").getOrCreate())
        spark.sparkContext.setLogLevel("WARN")
        report["runtime"] = {"spark_version": spark.version, "master": spark.sparkContext.master}
        monthly = validate_monthly_baseline(spark, args, recorded_monthly, report)
        frame = spark.read.parquet(input_dir.as_posix())
        actual = {field.name: field.dataType.simpleString() for field in frame.schema}
        expected = expected_cleaned_types()
        check(report, "full_cleaned_schema", actual == expected and len(frame.columns) == len(expected),
              {"expected": expected, "actual": actual})
        check(report, "required_input_schema", all(actual.get(c) == t for c, t in REQUIRED_TYPES.items()),
              {"expected": REQUIRED_TYPES, "actual_required": {c: actual.get(c) for c in REQUIRED_TYPES}})
        cached = frame.select(*REQUIRED_TYPES).persist(StorageLevel.MEMORY_AND_DISK)
        cached.createOrReplaceTempView("flights_cleaned")
        source = collect_source_months(spark, report)
        reconcile_source(source, phase2, monthly, report)
        report["input"]["status"] = "PASS"
        for name, filename in QUERIES.items():
            active, stage = name, name + ":validation"
            result = report["analyses"][name]
            result.update(status="RUNNING", sql_file=filename)
            df = spark.sql((ROOT / "sql/analytics" / filename).read_text(encoding="utf-8"))
            rows = bounded_rows(df, name, EXPECTED_MONTHS, report, name)
            if name == "delay_causes":
                validate_causes(rows, source, report)
            else:
                validate_performance(name, rows, source, report)
            stage = name + ":csv_export_read_back"
            path = output_dir / name
            result.update(row_count=len(rows), schema=df.schema.simpleString(), csv_path=path.as_posix())
            restored = export_csv(spark, df, path, rows, report, name)
            result["read_back_rows"] = len(restored)
            stage = name + ":mvp_regression"
            validate_mvp(spark, name, df.schema, restored, args, mvp_evidence, report)
            result["status"] = "PASS"
            print(f"\n{name}: {len(rows)} rows; validation, CSV read-back and MVP regression PASS")
            for row in rows[:10]:
                print(row)
        report["overall_status"] = "PASS"
    except Exception as error:
        report.update(overall_status="FAIL", failure_stage=stage)
        failed_section = report["preflight"] if stage == "preflight" else (
            report["input"] if active is None else report["analyses"][active])
        failed_section["status"] = "FAIL"
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
