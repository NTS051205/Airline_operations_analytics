# Phase 3A — KPI Definitions and Core Analytics

Status: **Implementation and runtime validation completed; pending Git commit.**
The owner-run pipeline returned exit code `0`; the actual Phase 3A validation JSON
records `PASS`. Three business queries executed bounded previews, not a complete
review of their three-month results. Phase 3B and Phase 4 have not started.

## 1. Scope and evidence

Flight data comes only from `data/processed/flights_cleaned/`. Phase 3A reads the
existing Phase 2 JSON at `artifacts/phase2/validation_summary.json` as a count
baseline; it does not read raw CSV or rerun Phase 1/2.

The owner-confirmed Phase 2 baseline is 1,645,503 records and 38 columns:
539,747 in January, 504,884 in February and 600,872 in March 2025;
1,611,046 completed, 30,640 cancelled and 3,817 diverted, with zero invalid statuses.
These prior-phase counts are the reconciliation baseline. The Phase 3A JSON now
confirms that source and aggregate counts match them. The runner loads baseline
counts from the Phase 2 JSON rather than substituting them for actual SQL results.

Current evidence: `artifacts/phase3/validation_summary.json`, with
`generated_at_utc = 2026-10-10T05:35:56.597113+00:00`. Validation/count/schema results
below were checked directly against this file. Exit code `0` is owner-confirmed;
the JSON does not contain the process exit code. Calculated percentages are
explicitly labeled as derived from recorded counts.

Phase 3A covers monthly, carrier, origin-airport and directed-route performance.
Day-of-Week, Scheduled Hour and Delay Cause Analysis belong to Phase 3B.
Power BI, ML, causal analysis and long-term trend claims are outside this step.

## 2. Grain and output schema

Four persisted tables were written and read back successfully under `data/analytics/`:

| Table | One row represents | Verified columns | Pre-write rows | Read-back rows |
|---|---|---:|---:|---:|
| `monthly_performance` | `YEAR, MONTH` | 15 | 3 | 3 |
| `airline_monthly_performance` | `YEAR, MONTH, OP_UNIQUE_CARRIER` | 16 | 42 | 42 |
| `origin_airport_monthly_performance` | `YEAR, MONTH, ORIGIN` | 16 | 991 | 991 |
| `route_monthly_performance` | `YEAR, MONTH, ORIGIN, DEST` | 17 | 16,997 | 16,997 |

Row counts come directly from `pre_write.tables.<table>.row_count` and
`read_back.tables.<table>.row_count`; column counts are counted from the recorded
schema maps, whose expected/actual names and types match. These are aggregate row
counts, not distinct-carrier, distinct-airport or distinct-route counts across the
entire quarter; the same entity can appear in multiple months.

`YEAR` and `MONTH` are Spark `int`; carrier and airport codes are `string`.
Each table has the same 13 measures: seven `bigint` counts, two `double` delay
measures (sum and average), and four `double` rates. Rates remain unrounded
fractions from 0 to 1; percentage formatting is a presentation concern.

Output uses Snappy Parquet, no directory partitions, and `coalesce(1)` for at most
one data part file per small aggregate table. Spark metadata files are additional.
The runner refuses an existing output directory or validation report. It never
overwrites cleaned input and never deletes partially written outputs.

Origin-airport performance describes arrival outcomes of flights departing that
airport. It does not attribute the cause of a delay to that airport. Routes are
directed: A → B and B → A are different groups, pooled across carriers.

## 3. KPI definitions and NULL handling

The source is one cleaned record per raw record. `flight_status` must agree with
`CANCELLED`/`DIVERTED`: completed = 0/0, cancelled = 1/0, diverted = 0/1.
Unexpected status, blank grouping key, invalid month/date or invalid arrival
label causes failure before any analytics output is written. No rows are silently
filtered to make validation pass.

| Output column | Definition / numerator | Denominator and NULL behavior |
|---|---|---|
| `total_flights` | All records in this group | Includes completed, cancelled and diverted |
| `completed_flights` | Count where status is `completed` | Count is zero if none |
| `cancelled_flights` | Count where status is `cancelled` | Count is zero if none |
| `diverted_flights` | Count where status is `diverted` | Count is zero if none |
| `delayed_arrival_flights` | Completed and `ARR_DEL15 = 1` | Official >=15-minute label |
| `on_time_arrival_flights` | Completed and `ARR_DEL15 = 0` | Under 15 minutes late, including early arrivals |
| `arrival_delay_rate` | `delayed_arrival_flights` | `completed_flights`; NULL if zero |
| `on_time_arrival_rate` | `on_time_arrival_flights` | `completed_flights`; NULL if zero |
| `cancellation_rate` | `cancelled_flights` | `total_flights`; NULL if zero |
| `diversion_rate` | `diverted_flights` | `total_flights`; NULL if zero |
| `arrival_delay_observed_flights` | Completed with non-NULL `ARR_DELAY` | Denominator for the signed average |
| `arrival_delay_minutes_sum` | Sum of `ARR_DELAY` on completed flights | SQL SUM ignores NULL; NULL when no observed values |
| `average_arrival_delay` | `AVG(ARR_DELAY)` on completed flights | Ignores NULL; NULL if no observed values |

Average arrival delay is signed: early arrivals can reduce the average. Negative
`ARR_DELAY` is valid, and the previously observed 3,407-minute extreme is retained.
This average is not the average positive delay among delayed flights.

Completed flights must have `ARR_DEL15` in {0,1}; cancelled/diverted must have NULL
for this label. Missing labels are never filled with zero. A completed flight with
NULL `ARR_DELAY` remains in the label-rate denominator but is excluded by SQL AVG;
the observation count makes that distinction visible. No imputation is performed.
NaN/infinite completed delay values fail validation. Conditional NULLs in
cancellation/delay-cause fields are not used or reclassified by Phase 3A.

Every division uses `NULLIF(denominator, 0)`. A NULL rate means undefined, not 0%.
No empty combinations of month/carrier/airport/route are manufactured.

For a later rollup, compute `SUM(delayed_arrival_flights) /
NULLIF(SUM(completed_flights), 0)`. For the signed average use
`SUM(arrival_delay_minutes_sum) / NULLIF(SUM(arrival_delay_observed_flights), 0)`.
Never use an unweighted average of stored rates or averages.

## 4. SQL implementation

- `000_kpi_summary.sql`: a CTE with four explicit `GROUPING SETS` computes all
  counts, SUM and AVG once in shared SQL. A final projection computes rates from
  those counts. `GROUPING()` assigns a readable level name; it does not infer the
  level from NULL dimension values.
- `001`–`004`: select only the relevant level and columns for each output. They
  have no volume filter, so all valid groups remain available for reconciliation.
- `005`–`007`: query validated Parquet read-back views; no new persisted tables.
- `scripts/build_analytics.py`: Spark setup, paths, SQL execution, validation
  orchestration, four Parquet writes/read-backs, bounded console previews and JSON.
- `src/airline_analytics/analytics_validation.py`: source/schema, grain, count,
  formula, NULL and monthly reconciliation checks. It reuses the existing Phase 2
  cleaned type contract without applying any cleaning transformations.

`flights_cleaned`, `kpi_summary` and the output views are session-only views.
Only the grouped summary is persisted in Spark's memory-and-disk cache, and is
released when the run ends. GROUPING SETS produces four summaries of each flight;
it is not a new flight-level dataset to sum across. No warehouse/catalog is built.
Local settings are eight shuffle partitions, UTC and ANSI mode with guarded
division. Use the owner-verified `local[2]` and `4g` driver settings in the README.
This is a resource-conscious configuration, not a measured performance claim.

## 5. Three business analyses

### Airline Performance Ranking — `005_airline_ranking.sql`

Question: Among carriers with sufficient completed-flight volume, which have
lower observed arrival delay rates within the same month?

An eligible-carrier CTE applies `:min_completed_flights`; `DENSE_RANK()` partitions
by YEAR/MONTH and orders the unrounded rate ascending. Rank 1 is the lowest rate.
Equal rates share a rank. A LEFT JOIN retains all carrier/month groups, including
small groups with NULL rank. Completed count, delayed count, rate and the volume
flag remain visible. Carrier code provides stable display ordering only, not a
tie-break in the rank calculation.

The default shared volume heuristic is 30 completed flights/month and can be
changed with `--min-completed-flights`. Passing this threshold does not establish
statistical significance. Interpret counts with rates; a carrier rank is not a
causal measure or an adjustment for its route mix.

### Month-over-Month Performance — `006_airline_mom.sql`

Question: How did each carrier's observed delay rate change from the immediately
preceding calendar month?

`LAG()` over carrier, ordered by YEAR/MONTH, retrieves the previous observation's
month, counts and rate. MoM differences are populated only if that month equals
`ADD_MONTHS(current_month, -1)`. January or a missing immediately preceding month
therefore yields NULL differences; March is not compared to January as if adjacent.
The previous observation itself remains visible with its date for inspection.

- `delay_rate_change_pp = 100 × (current_rate − previous_rate)`.
- `delay_rate_relative_change_pct = 100 × (current_rate − previous_rate) /
  previous_rate`, only for a positive previous rate.

A zero previous rate permits the percentage-point difference but leaves relative
change NULL. Undefined rates propagate NULL. Positive differences mean more delay;
negative differences mean less delay. `both_months_meet_volume_heuristic` highlights
comparisons needing volume caution without filtering the history before LAG.
Three months do not establish seasonality or a long-term trend.

### High observed-delay routes — `007_high_delay_routes.sql`

Question: Which directed routes have comparatively high observed delay rates and
at least 30 completed flights within a month?

An eligible-route CTE filters the already aggregated route table using the same
adjustable volume heuristic. `DENSE_RANK()` within each month orders the unrounded
rate descending. Completed/delayed counts and the rate accompany the rank.
WHERE is appropriate because the source already has the required grain;
an extra GROUP BY/HAVING at that grain would perform a redundant aggregation.

There is no absolute "high risk" cutoff or prediction. Specific route findings
must be tied to the actual preview rows. The query does not filter on an
invented statistical threshold, drop reverse routes, or claim why delays occurred.
All eligible routes are part of the SQL result; the runner prints a bounded preview,
not an exhaustive report. Three previews are not three additional saved tables.

## 6. Validation contract

Before writes, the runner must verify:

1. All 38 source column names/types match the Phase 2 contract; required grouping
   keys/date/status/arrival labels satisfy the input rules.
2. Source count and monthly total reconcile; monthly total/month counts and global
   completed/cancelled/diverted counts equal the successful Phase 2 JSON baseline.
3. Each output has its expected schema and a unique, non-null grain. Count values
   are non-null and nonnegative; status counts sum to total and arrival-label
   counts sum to completed. Observation counts cannot exceed completed flights.
4. Rates match counts and their correct denominators, including NULL when the
   denominator is zero and finite 0–1 values otherwise. Average matches the signed
   sum/non-null observation count; it is not restricted to 0–1 or nonnegative.
5. Airline, origin and route tables reconcile by month to the monthly table for
   all additive counts and signed delay sums. Rates are never summed or averaged.

After writes, each table is read back and undergoes the same contract checks.
Its row count must match pre-write. Bidirectional `EXCEPT ALL` also checks that
all values and their multiplicities are unchanged. Comparison aligns columns by
name; Parquet nullable metadata and column ordering are not semantic differences.

Counts and stored read-back values compare exactly. Floating-point formula checks
use relative tolerance `1E-12` with scale at least 1; rollup sum checks additionally
allow absolute tolerance `1E-8` for summation order. This is numerical tolerance,
not rate rounding or data correction.

The owner-run job wrote `artifacts/phase3/validation_summary.json`. The outline
below describes its fields and possible statuses; verified values from this run
are documented in section 9.

```text
phase: "3A"
overall_status: RUNNING | PASS | FAIL
failure_stage: null or stage name
generated_at_utc, input_path, phase2_report_path, output, volume_heuristic
runtime: spark_version, master, shuffle_partitions
preflight: status, phase2_baseline
input: status, schema, row_count, violations, checks, failed_checks
pre_write:
  status
  baseline: status, baseline, actual_totals, checks, failed_checks
  tables:
    <one entry per output>: status, schema, row_count, by_month,
                           violations, checks, failed_checks
write: status, tables: <per-table write status>
read_back: status, tables: <schema/count/value and reconciliation evidence>
business_queries: status, queries: <PREVIEW_EXECUTED per query when reached>
errors: <recorded exceptions, if any>
limitations: <scope of checks and interpretation>
```

Stages not reached remain `NOT_RUN`; metrics for those stages are not invented.
Argument/path preflight refusals occur before report creation. Later exceptions
produce FAIL and a nonzero exit; partial outputs remain for owner inspection.
Neither a successful write nor passing reconciliation proves perfect data, correct
causal interpretation, or production performance. Aggregate reconciliation can
miss semantic errors shared by all groups, so owner review of SQL remains required.

## 7. Completed execution and documentation handoff

PowerShell environment/run/report commands are in README section 15. No dependency
or technology is added. Python 3.11.9, PySpark 3.5.8, Java 17 and the existing Windows
Hadoop utilities are retained. Both PYSPARK variables must point to `.venv` Python.

The owner has run and verified the job successfully. The implementation and runtime
validation are complete; Git commit is pending. Monthly findings in section 10 use
recorded JSON counts. The JSON does not retain the three console preview datasets,
so carrier ranking, AA MoM and January route examples need the existing preview
rows before they can be documented. Full three-month business-query interpretation
has not been completed. No Phase 3B, dashboard or model is prepared here.

Avoid these double-counting pitfalls:

- Do not UNION and sum the four outputs: each represents the same source flights.
- Do not join separate carrier and route aggregates on month alone; many-to-many
  multiplication inflates counts. These outputs do not support a carrier-by-route
  cross-filter at their current grains.
- Recompute rolled-up rates from summed numerators/denominators; similarly weight
  averages by observed-delay counts. Keep YEAR alongside MONTH.
- Exclude cancelled/diverted only from arrival KPIs, not from total-flight counts.

## 8. Interview practice

1. **Why keep counts with rates?** They allow valid weighted rollups; an average
   of group percentages gives small groups too much weight.
2. **Why GROUPING SETS?** It expresses four explicit grains using one definition
   of the KPI calculations. GROUPING identifies which dimensions are rolled up.
3. **Why check the date after LAG?** LAG returns the previous observed row, which
   may not be the immediately preceding calendar month.
4. **Why DENSE_RANK and a volume flag?** Equal observed rates share a rank; the
   count exposes small-sample limitations that rank alone would hide.
5. **Why read back Parquet?** A successful write call alone does not verify the
   stored schema, counts or values against the pre-write aggregates.

## 9. Verified Phase 3A runtime results

| Evidence | Verified value | Source |
|---|---|---|
| Pipeline exit code | 0 | Project owner confirmation |
| Overall / failure stage | PASS / null | `overall_status`, `failure_stage` |
| Input records | 1,645,503 | `input.row_count` |
| Input schema | 38 columns; expected and actual names/types match | `input.schema` |
| Preflight / input / pre-write / write / read-back | All PASS | Respective stage `status` fields |
| Monthly total/status/month reconciliation | PASS | `pre_write.baseline.checks` |
| All four read-back datasets unchanged | true for every table | `read_back.tables.<table>.checks.all_values_unchanged` |
| Failed checks | 0 | All recorded check booleans true and all `failed_checks` arrays empty |
| Errors | 0 | `errors` is empty |
| Spark runtime | 3.5.8; local[2]; 8 shuffle partitions | `runtime` |
| Output configuration | Parquet, Snappy, no directory partitions | `output` |
| Volume heuristic | 30 completed flights/month | `volume_heuristic` |
| Three business queries | Each PREVIEW_EXECUTED | `business_queries.queries` |

The checks passed for nonnegative counts, non-null/unique grain, status and label
reconciliation, rate denominators and formulas, conditional NULL handling, signed
delay average, and monthly reconciliation of every aggregate. All four read-backs
matched pre-write row counts and schemas and passed bidirectional `EXCEPT ALL`,
which includes value multiplicities. The output row counts are in section 2.

`pre_write.baseline.actual_totals` directly records 1,611,046 completed, 30,640
cancelled, 3,817 diverted, 317,266 delayed arrivals and 1,293,780 on-time arrivals.
Checks are scoped to the implemented rules: they do not prove perfect data,
causality, production readiness or benchmarked performance.

## 10. Business Findings and evidence boundaries

### Monthly delay and cancellation performance

The following counts are direct values from
`read_back.tables.monthly_performance.by_month`. Percentages are calculated from
those counts for documentation and rounded to two decimal places. They are not
rate fields directly serialized in the JSON, and no Spark query was rerun to
produce this documentation.

| Month (2025) | Total flights | Completed | Delayed arrivals | Arrival Delay Rate | Cancelled | Cancellation Rate |
|---|---:|---:|---:|---:|---:|---:|
| January | 539,747 | 522,269 | 98,130 | 18.79% | 16,312 | 3.02% |
| February | 504,884 | 496,476 | 103,102 | 20.77% | 7,405 | 1.47% |
| March | 600,872 | 592,301 | 116,034 | 19.59% | 6,923 | 1.15% |

Arrival Delay Rate = delayed arrivals / completed flights. Cancellation Rate =
cancelled flights / total flights. Counts and denominators must be read together:
March has more delayed arrivals than February but a lower arrival delay rate.
February's arrival delay rate is the highest of the three observed months;
cancellation rates decrease across January, February and March. These are observed
comparisons within this MVP, not evidence of causes, predictions or long-term trends.

### Airline ranking, AA MoM and January route examples

The validation JSON confirms that `005_airline_ranking.sql`, `006_airline_mom.sql`
and `007_high_delay_routes.sql` executed successfully as bounded previews. It does
not include the preview rows or the complete result sets. The airline and route
tables' `by_month` report sections contain all-entity monthly rollups, not
individual carrier/route measurements.

| Requested example | Current evidence boundary |
|---|---|
| Airline ranking examples | Await existing preview rows with month, carrier, completed/delayed counts, unrounded rate and rank |
| AA month-over-month example | Await AA preview rows with both months' counts/rates, consecutive-month flag, percentage-point difference and relative change |
| January high-delay routes | Await January preview rows with directed ORIGIN/DEST, completed/delayed counts, rate and rank under the 30-flight heuristic |

No specific airline rank, AA change or route rate is inferred from global totals.
Preview execution is not full evaluation of all three months. Once those existing
rows are supplied, any examples must remain explicitly limited to the observed
preview, show flight volume, and avoid causal or predictive language. Meeting the
30-flight heuristic alone does not establish statistical significance.
