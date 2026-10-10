# Phase 3B — Additional Business Analytics

**Status: Implementation and runtime validation completed; pending Git commit.**
The owner completed the pipeline with exit code 0. The actual validation JSON and
all three generated CSV outputs were read to finalize this report; no job was rerun.

Phase 3A is complete and was pushed to `main` at `5aa27ea`, as confirmed by the
owner. Its four analytical Parquet tables and its code are not changed or rebuilt.
Power BI and Machine Learning have not started.

## 1. Scope, files and data flow

Input: `data/processed/flights_cleaned/`, the existing January–March 2025 cleaned
Parquet dataset. Prior phases verified 1,645,503 records: 1,611,046 completed,
30,640 cancelled and 3,817 diverted. Phase 3B independently recorded 1,645,503 input
rows and monthly total/completed/delayed counts (section 7). Cancelled/diverted
counts above remain prior-phase evidence, not separate Phase 3B JSON measurements.
The runner uses actual source monthly counts as reconciliation references.

Exactly three analyses were executed and validated:

| File | Responsibility | Output grain |
|---|---|---|
| `sql/analytics/008_day_of_week.sql` | Observed arrival delay rate by BTS weekday | YEAR, MONTH, DAY_OF_WEEK |
| `sql/analytics/009_departure_time_band.sql` | Observed arrival delay rate by scheduled departure band | YEAR, MONTH, time_band |
| `sql/analytics/010_delay_causes.sql` | Shares of reported attributed delay minutes | YEAR, MONTH, cause_category |
| `scripts/run_phase3b.py` | Read Parquet, run SQL, validate, export/read CSV, record JSON | Orchestration only |
| `docs/phase3b_findings.md` | Definitions, limitations, execution handoff and findings | Documentation |

The runner registers `flights_cleaned` as a temporary Spark SQL view. Only the 11
required input columns are cached with memory-and-disk fallback. It does not read
raw CSV or Phase 3A outputs and never writes into `data/analytics/`.

Verified user-run artifacts:

```text
artifacts/phase3b/
  day_of_week/                 # header + one CSV data part file; Spark metadata
  departure_time_band/         # header + one CSV data part file; Spark metadata
  delay_causes/                # header + one CSV data part file; Spark metadata
  validation_summary.json
```

Each CSV output is a Spark directory, not a file named after the directory. NULL
is serialized as the literal `NULL` and read back with the same setting. Aggregated
results contain 21 weekday, 12 time-band and 15 cause rows in this run.
Collecting these bounded aggregates for validation/preview does not collect the
flight-level dataset. There is no `toPandas()` call.

## 2. Day-of-Week Performance

**Business question:** Which weekdays show higher observed arrival delay rates?

SQL groups directly by YEAR, MONTH and the existing DAY_OF_WEEK. BTS coding is
retained: 1 Monday, 2 Tuesday, 3 Wednesday, 4 Thursday, 5 Friday, 6 Saturday,
7 Sunday. The runner rejects NULL/out-of-domain weekday values before exporting;
it does not silently filter flights or recalculate weekday from timestamps.

Verified output schema (7 columns):

- `YEAR`, `MONTH`, `DAY_OF_WEEK`: int, forming the unique grain.
- `total_flights`, `completed_flights`, `delayed_arrival_flights`: bigint counts.
- `arrival_delay_rate`: double, delayed completed flights / completed flights.

All flight statuses contribute to total flights. Only `flight_status='completed'`
contributes to completed flights, and only completed with `ARR_DEL15=1` contributes
to delayed arrivals. `NULLIF(completed_flights, 0)` gives NULL when the rate is
undefined. A day with more delayed flights can still have a lower delay rate if
it has more completed flights. Compare rates together with their denominators.

Counts across weekday groups must reproduce the source total/completed/delayed
counts in each month. The check preserves the source weekday coding; it does not
claim an independent re-audit of its agreement with FL_DATE, which was outside
this supplementary analysis.

## 3. Scheduled Departure Time Bands

**Business question:** Does observed arrival delay rate differ across scheduled
departure time bands?

Use `CRS_DEP_TIME`, the scheduled local departure HHmm string. Actual `DEP_TIME`
is not used. No source field is changed or converted to an integer.

| Band | Valid scheduled times |
|---|---|
| Night | 0000–0559; also the exact BTS value 2400 |
| Morning | 0600–1159 |
| Afternoon | 1200–1759 |
| Evening | 1800–2359 |
| Missing | CRS_DEP_TIME is NULL |
| Invalid | Non-NULL values that are neither a valid four-digit HHmm nor 2400 |

The SQL validates all four characters before comparing HHmm strings. This retains
leading zeros and prevents malformed values from entering a normal band. `2400`
is treated as midnight for grouping only: it stays in the original YEAR/MONTH,
without inventing a new date or timestamp. Blank strings are Invalid if present;
the already-cleaned input normally represents blanks as NULL.

Verified output schema (8 columns): `YEAR`, `MONTH` (int); `time_band` (string); the same
three bigint flight counts and double rate as the weekday output; plus bigint
`scheduled_2400_flights` as a diagnostic. That count may be nonzero only in Night.

Missing and Invalid groups are retained whenever observed. They contribute to
monthly reconciliation and have the same status-specific numerator/denominator
rules. No zero-filled groups are manufactured for bands absent from a month.

Interpret differences as observed associations, not proof that the scheduled time
causes delay. Times are local to origin airports, not a common UTC time zone;
carrier, route and airport composition can differ between bands. A small band
requires volume caution, even if its observed rate is high.

## 4. Delay Cause Breakdown

**Business question:** Among completed flights labeled ARR_DEL15=1, which BTS
reporting categories account for the largest shares of attributed delay minutes?

The eligible CTE filters to exactly that population. `STACK` expands each eligible
flight into five category rows, retaining NULL values. Categories use the source
names CARRIER_DELAY, WEATHER_DELAY, NAS_DELAY, SECURITY_DELAY and
LATE_AIRCRAFT_DELAY. Grouped SUM/COUNT produce category minutes and coverage.
A window SUM over YEAR/MONTH supplies the common monthly denominator.

Verified output schema (10 columns):

| Column | Spark type | Meaning |
|---|---|---|
| YEAR, MONTH | int | Source flight month |
| cause_category | string | One of the five BTS source cause columns |
| eligible_delayed_flights | bigint | Completed ARR_DEL15=1 flights in this month |
| reported_cause_flights | bigint | Eligible flights with a non-NULL value for this category, including reported zero |
| flights_with_any_reported_cause | bigint | Eligible flights with at least one non-NULL cause field |
| flights_with_all_reported_causes | bigint | Eligible flights with all five cause fields non-NULL |
| attributed_minutes | double | SUM of the reported minutes in this category; numerator |
| monthly_attributed_minutes | double | SUM of all five category totals in this month; denominator |
| cause_minute_share | double | Numerator / NULLIF(denominator, 0); fraction from 0 to 1 |

The three month-level coverage counts and the monthly denominator repeat on each
category row. Do not add them across the five rows: one flight can contribute to
multiple reporting categories. `reported_cause_flights / eligible_delayed_flights`
describes reporting coverage, not the fraction of flights delayed due to that cause.
A reported zero counts as available reporting but adds no minutes.

NULL is never imputed to zero. SUM ignores individual NULLs; a category with no
reported values has NULL numerator, zero reported count and NULL share. A category
with reported zero-minute values can have a zero numerator, distinguishable by its
positive reported count. If all category totals are NULL, the monthly denominator
is NULL. If reported totals sum to zero, it is zero. Shares remain NULL in either
case. For a positive denominator, the available category shares sum to about 1
(100%); this alone does not demonstrate complete reporting coverage.

For a month with eligible flights, all five category rows are retained even when
some causes are unreported. A month with no eligible flights produces no cause
rows. Numerators and denominators describe attributed minutes only. They are not
compared or forced to equal signed ARR_DELAY, which is not read by this analysis.
No causal inference beyond the BTS reporting categories is claimed.

## 5. Focused validation and failure behavior

The runner checks actual execution results for:

1. Required input names/types, nonempty input and the January–March 2025 period.
   Weekday domain, flight status and conditional ARR_DEL15 labels must be valid.
2. Reported cause values must be finite and nonnegative; NULL is permitted. Input
   cause sums are also checked for finite results before they enter the report.
3. Unique output grains, valid group labels, nonnegative ordered flight counts,
   rate formulas/bounds and NULL rates for zero completed-flight denominators.
4. Monthly total/completed/delayed count reconciliation for weekday and time band.
   Separate source boundary counts check each normal time band's total, plus
   Missing/Invalid counts and the count/location of literal 2400.
5. Exact five-category coverage where eligible flights exist, coverage counts and
   category sums reconciled to independent source aggregates, shared denominator
   correctness and shares summing to approximately 1 only with positive minutes.
6. CSV write followed by typed read-back with header checking and FAILFAST parsing;
   both row count and all row values/multiplicities must match the small SQL result.

CSV has no stored type metadata. Read-back uses the SQL result schema explicitly;
it tests parsing and value preservation, not automatic inference of CSV types.
Counts compare exactly. Floating-point comparisons use relative tolerance 1E-12
and absolute tolerance 1E-9. This is numerical tolerance, not rounding in the SQL.

`validation_summary.json` records overall status, failure stage, runtime, source
monthly counts/quality diagnostics, per-analysis schema/row counts/CSV paths,
individual checks, failed check names and errors. Source cause reporting coverage
and sums are recorded alongside monthly counts. Verified results are in section 7.
Only successful execution and checks allow a PASS status. Unreached analyses stay
NOT_RUN. A failed check raises an exception and the process exits nonzero.

The output directory must be a new directory below this repository's `artifacts/`;
it cannot overlap input. Existing directories are refused even if empty. An early
path refusal creates no new report. Once a run reserves its output directory,
runtime failure preserves partial CSVs and records failure evidence where writable.
The runner performs no automatic deletion, append or overwrite. This successful
local run covers the implemented checks only, not perfect data or production performance.

## 6. Owner-run PowerShell commands — retained for reproducibility

The owner has completed execution. The commands below document the workflow,
not a request to rerun it. Keep the existing Python 3.11.9, PySpark 3.5.8,
Java 17 and working Windows Hadoop utilities. Codex has not run these commands.

```powershell
Set-Location "E:\airline_operations_analytics"

$projectPython = (Resolve-Path ".\.venv\Scripts\python.exe").Path
$sparkSubmit = (Resolve-Path ".\.venv\Scripts\spark-submit.cmd").Path
$env:PYSPARK_PYTHON = $projectPython
$env:PYSPARK_DRIVER_PYTHON = $projectPython

& $projectPython --version
java -version
& $projectPython -c "import pyspark; print(pyspark.__version__)"
Test-Path ".\data\processed\flights_cleaned"
Test-Path ".\artifacts\phase3b"

& $sparkSubmit `
    --master "local[2]" `
    --driver-memory "4g" `
    ".\scripts\run_phase3b.py" `
    --input-dir ".\data\processed\flights_cleaned" `
    --output-dir ".\artifacts\phase3b"

$phase3bExitCode = $LASTEXITCODE
Write-Host "Phase 3B exit code: $phase3bExitCode"
Get-Content ".\artifacts\phase3b\validation_summary.json" -Raw
Get-ChildItem ".\artifacts\phase3b" -Recurse -Filter "*.csv"
```

The cleaned input and `artifacts/phase3b` now exist. An identical rerun is refused
to protect the verified results. Any owner-authorized future rerun needs a fresh
directory such as `artifacts/phase3b_retry01`; there is no automatic deletion.
Both PYSPARK variables point at `.venv` Python for driver/worker consistency.

## 7. Verified runtime results and business findings

### Runtime and validation

Evidence: `artifacts/phase3b/validation_summary.json`, generated at
`2026-10-10T06:38:06.713552+00:00`, and the CSV data part files in its three
output directories. Counts, minutes and unrounded row rates/shares below come
directly from JSON/CSV. Percent displays multiply stored fractions by 100 and
round to two decimals. Totals/coverage percentages explicitly marked derived
are arithmetic from those records, not additional Spark checks.

| Runtime / validation | Verified result |
|---|---|
| Pipeline exit code | 0 (owner-confirmed; not a JSON field) |
| Overall / input status | PASS / PASS |
| Spark / master | 3.5.8 / local[2] |
| Input rows | 1,645,503 |
| Validation checks | 30/30 true (counted from JSON checks) |
| Failed checks / errors | [] / [] |
| Failure stage | null |

| CSV output directory under artifacts/phase3b/ | Output / read-back rows | Result |
|---|---:|---|
| day_of_week/ | 21 / 21 | PASS |
| departure_time_band/ | 12 / 12 | PASS |
| delay_causes/ | 15 / 15 | PASS |

All three CSV headers/data were inspected. JSON records successful required-input
schema, domain/grain, count/rate reconciliation, time-band boundaries, cause
coverage/minutes/share checks and CSV row-count/value read-back checks. No CSV
type inference is claimed: read-back used each SQL result's explicit schema.

Source counts from `input.by_month`:

| Month (2025) | Total flights | Completed flights | Delayed completed flights |
|---|---:|---:|---:|
| January | 539,747 | 522,269 | 98,130 |
| February | 504,884 | 496,476 | 103,102 |
| March | 600,872 | 592,301 | 116,034 |
| Three-month sum (derived) | 1,645,503 | 1,611,046 | 317,266 |

Both weekday and time-band outputs reconcile to these monthly counts. Scheduled
departure Missing, Invalid and literal 2400 each have **0 observed rows in every
month**, as recorded by `missing_times`, `invalid_times` and `midnight_2400`.
Consequently only four normal bands appear per month (12 rows total), and all
CSV `scheduled_2400_flights` counts are zero. This run does not exercise nonempty
Missing/Invalid/2400 cases. All observed completed-flight denominators and monthly
attributed-minute denominators are positive; zero-denominator branches were not
exercised by these outputs either.

### Day-of-week observations

Source: `artifacts/phase3b/day_of_week/`. Comparing all seven unrounded rates
within each month gives:

| Month (2025) | Position by observed rate | Weekday | Delayed / completed flights | Arrival delay rate |
|---|---|---|---:|---:|
| January | Highest | Monday (1) | 16,002 / 70,684 | 22.64% |
| January | Lowest | Wednesday (3) | 10,586 / 79,713 | 13.28% |
| February | Highest | Thursday (4) | 21,753 / 76,272 | 28.52% |
| February | Lowest | Tuesday (2) | 10,618 / 65,833 | 16.13% |
| March | Highest | Sunday (7) | 25,738 / 98,176 | 26.22% |
| March | Lowest | Tuesday (2) | 9,429 / 72,141 | 13.07% |

The highest-rate weekday changes across months, so these results do not support
calling one weekday consistently worst. January Friday had more delayed arrivals
(17,196 / 88,355 = 19.46%) than Monday but a lower rate. Volume and rate answer
different questions; no average of weekday percentages is used for a monthly rate.

### Scheduled-time-band observations

Source: `artifacts/phase3b/departure_time_band/`. Every cell shows delayed /
completed flights followed by its arrival delay rate.

| Month (2025) | Night | Morning | Afternoon | Evening |
|---|---|---|---|---|
| January | 1,811 / 14,430 (12.55%) | 32,214 / 207,726 (15.51%) | 39,551 / 191,227 (20.68%) | 24,554 / 108,886 (22.55%) |
| February | 1,781 / 14,282 (12.47%) | 32,082 / 195,866 (16.38%) | 42,221 / 180,233 (23.43%) | 27,018 / 106,095 (25.47%) |
| March | 1,732 / 19,989 (8.66%) | 30,416 / 228,314 (13.32%) | 47,542 / 206,404 (23.03%) | 36,344 / 137,594 (26.41%) |

Evening has the highest observed rate and Night the lowest in each of the three
months. Night also has substantially fewer completed flights. This is a descriptive
comparison of scheduled local-time groups, not evidence that a later departure
causes delay or a recommendation to reschedule flights without further analysis.

### Delay-cause observations

Source: `artifacts/phase3b/delay_causes/`. The largest recorded category in each
month, comparing all five unrounded minute shares, is:

| Month (2025) | Largest category | Category minutes / monthly attributed minutes | Minute share |
|---|---|---:|---:|
| January | CARRIER_DELAY | 2,510,886 / 6,943,994 | 36.16% |
| February | LATE_AIRCRAFT_DELAY | 2,735,511 / 7,411,011 | 36.91% |
| March | LATE_AIRCRAFT_DELAY | 3,281,080 / 8,378,368 | 39.16% |

Carrier and late-aircraft categories are the two largest in each month. January
late-aircraft minutes are 2,410,759 / 6,943,994 (34.72%); February and March carrier
minutes are 2,513,734 / 7,411,011 (33.92%) and 2,761,618 / 8,378,368 (32.96%).
These are shares of attributed minutes among completed ARR_DEL15=1 flights,
**not percentages of delayed flights caused by a category**.

For every category, reported-cause count equals eligible delayed-flight count:
January 98,130 / 98,130, February 103,102 / 103,102 and March 116,034 / 116,034.
Any-cause and all-five-causes counts also equal those eligible counts. Thus
non-NULL coverage is 100% in each month (derived), including reported zeros.
This does not mean every category has positive minutes on every flight or that
the reporting categories explain all operational causes. NULL handling remains
as defined above, but absent cause-field coverage is not observed in this eligible
population. Monthly shares sum to approximately 100% under the recorded checks;
the repeated monthly denominators must not be added across category rows.

### Interpretation limits and completion boundary

These findings describe January–March 2025 only. They do not establish a long-term
trend, recurring seasonal pattern, causal effect or predictive performance.
Carrier, airport and route composition may differ between weekday/time groups;
no adjustment for those differences or statistical significance analysis was
performed here. Completed-only delay rates exclude cancelled/diverted flights
and do not measure total passenger disruption. Reporting coverage is not proof
of perfect data, and validation PASS is not a production benchmark.

Implementation and runtime validation are completed; Git commit is pending.
Only these findings and README were finalized. Power BI and Machine Learning
remain not started.
