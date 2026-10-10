# Phase 1 Data Quality Report

> **Status:** Phase 1 is complete, confirmed by the project owner, and committed/pushed in Git commit `3403d4d`.

## 1. Scope and evidence

This report covers the three BTS Reporting Carrier On-Time Performance CSV files for January, February, and March 2025. The project owner ran the PySpark 3.5.8 profiling job successfully with exit code `0` and reviewed `artifacts/phase1/profile_summary.json`.

The report uses two evidence labels:

- **JSON direct:** a value recorded directly in `profile_summary.json`.
- **Derived from JSON:** arithmetic calculated only from values recorded in that JSON. Derived values do not represent an additional Spark validation.

No cleaning, KPI aggregation, Power BI modeling, or machine-learning work is included in this phase.

## 2. Dataset coverage

| Period | Records | Source |
|---|---:|---|
| January 2025 | 539,747 | JSON direct |
| February 2025 | 504,884 | JSON direct |
| March 2025 | 600,872 | JSON direct |
| **January–March 2025** | **1,645,503** | **JSON direct** |

- Columns in the verified input schema: **36** — JSON direct.
- Monthly reconciliation: `539,747 + 504,884 + 600,872 = 1,645,503` — derived from JSON.

## 3. Flight-status distribution

| Status | Records | Source |
|---|---:|---|
| Completed | 1,611,046 | JSON direct |
| Canceled | 30,640 | JSON direct |
| Diverted | 3,817 | JSON direct |
| **Total** | **1,645,503** | **Derived from JSON** |

The status counts reconcile to the raw record count: `1,611,046 + 30,640 + 3,817 = 1,645,503`. This is a count reconciliation, not a business-performance conclusion.

## 4. Schema and parsing checks

| Check | Result | Interpretation |
|---|---:|---|
| Expected columns | 36 | The three files matched the Phase 1 schema contract. |
| Cast failures | 0 | No cast failures were recorded for the configured typed columns. This confirms parseability under the configured casts, not that every value is semantically perfect. |

## 5. Missing values

Missingness must be interpreted in the context of flight status and BTS field semantics. A `NULL` is not automatically a data-quality error.

| Observation | Value | Source | Interpretation |
|---|---:|---|---|
| Missing `ARR_DEL15` | 34,457 | JSON direct | The count numerically matches the aggregate number of canceled and diverted flights. |
| Canceled + diverted | `30,640 + 3,817 = 34,457` | Derived from JSON | Aggregate reconciliation only; it does not by itself prove row-level identity. |
| Violations of configured completed-flight required-field rules | 0 | JSON direct | No violation was detected by those specific rules. |

Expected or condition-dependent `NULL` patterns include:

- `CANCELLATION_CODE` is applicable to canceled flights; its absence on a non-canceled flight is not an error by default.
- `CARRIER_DELAY`, `WEATHER_DELAY`, `NAS_DELAY`, `SECURITY_DELAY`, and `LATE_AIRCRAFT_DELAY` are condition-dependent delay-cause fields. Their absence outside applicable delayed-flight records is not an error by default.
- Actual departure/arrival and delay fields can be absent when a flight does not reach the corresponding normal operating milestone. Their validity must therefore be judged with `CANCELLED`, `DIVERTED`, and the configured status-specific rules.

This report does not infer an exact missing count for a conditional field unless that count is directly present in the JSON output.

## 6. Duplicate checks

| Metric | Result | Source |
|---|---:|---|
| Exact duplicate groups | 0 | JSON direct |
| Exact rows in duplicate groups | 0 | JSON direct |
| Exact excess duplicate rows | 0 | JSON direct |
| Candidate-key collision groups | 0 | JSON direct |
| Rows in candidate-key collision groups | 0 | JSON direct |
| Candidate-key excess rows | 0 | JSON direct |

The three duplicate measures have different meanings: groups count repeated identities, rows-in-groups count every row belonging to those groups, and excess rows count only rows beyond the first occurrence. The candidate-key result applies only to the Phase 1 candidate key used by the profiler; it does not establish an official BTS primary key.

## 7. Configured anomaly rules

- Confirmed-invalid-rule violations: **0** — JSON direct.
- Suspicious-business-rule violations: **0** — JSON direct.

This means no violations were detected by the rules implemented in Phase 1. It must not be interpreted as proof that the dataset is error-free or that unimplemented rules would also return zero.

## 8. Extreme value requiring review

- Maximum observed `ARR_DELAY`: **3,407 minutes** — JSON direct.

This is an extreme value that should remain visible for later investigation. Phase 1 does not label it invalid, cap it, remove it, or claim a cause without supporting evidence.

## 9. Descriptive rate derived from the profile

- Completed flights with `ARR_DEL15 = 1`: **317,266** — JSON direct.
- Arrival Delay Rate among completed flights: `317,266 / 1,611,046 ≈ 19.69%` — derived from JSON.

The denominator is explicitly the **1,611,046 completed flights**, not all 1,645,503 raw records. This is a descriptive profiling statistic, not yet a finalized business KPI.

## 10. Data-quality limitations

- Zero detected violations only covers the schema, duplicate definitions, casts, and anomaly rules currently implemented.
- Aggregate count equality does not establish row-level equivalence unless a row-level rule tested it.
- Zero cast failures establishes successful parsing under the configured types, not real-world correctness.
- The candidate key is a project-level duplicate candidate, not a formally declared BTS key.
- Profiling does not validate every possible cross-field relationship, external reference value, or operational edge case.
- No cleaning, imputation, outlier treatment, Parquet read/write validation, KPI layer, Power BI model, or machine-learning evaluation was performed in Phase 1.

## 11. Phase status

Phase 1 is formally closed and remains the evidence baseline for raw-data profiling. Phase 2 cleaning and Parquet validation subsequently completed successfully; its runtime results are documented separately in `docs/phase2_cleaning_report.md` and `artifacts/phase2/validation_summary.json`.
