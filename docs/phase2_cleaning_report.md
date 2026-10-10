# Phase 2 Cleaning and Parquet Report

> **Status:** Completed and confirmed by the project owner; committed and pushed (code `6c50b00`, documentation `4a9e907`, as reported by the owner).

## 1. Scope and evidence boundary

Phase 2 prepares this local PySpark flow:

```text
BTS raw CSV -> typed cleaning -> pre-write validation
            -> partitioned Parquet -> read-back validation
```

The verified Phase 1 baseline remains `artifacts/phase1/profile_summary.json`. The project owner ran both Phase 2 commands and supplied the verified results from `artifacts/phase2/validation_summary.json`. Metrics in this report describe that run; they are not inferred from code inspection.

## 2. Input and output

| Purpose | Default path | Behavior |
|---|---|---|
| Raw input | `data/raw/` | Three verified BTS CSV files; never modified |
| Phase 1 baseline | `artifacts/phase1/profile_summary.json` | Read-only count baseline |
| Cleaned output | `data/processed/flights_cleaned/` | Successfully written and read back |
| Validation report | `artifacts/phase2/validation_summary.json` | Runtime evidence reviewed by the project owner |
| Parquet smoke output | `artifacts/phase2/parquet_smoke_test/` | Small environment test completed successfully |

Parquet uses Snappy compression and partitions by `YEAR` and `MONTH`. The three-month MVP is repartitioned to three shuffle partitions before the write to limit small files without forcing all data into one file.

### Verified runtime environment

| Item | Verified result |
|---|---|
| OS | Windows 11 |
| PySpark | 3.5.8 |
| Windows Hadoop utilities | Hadoop 3.3.4 utilities configured |
| Parquet smoke test | PASS; exit code `0` |
| Full cleaning pipeline | PASS; exit code `0` |

The successful run verifies local Parquet write/read for this project environment. Hadoop utility binaries remain local environment prerequisites and are not committed to Git.

## 3. Cleaning rules

### Types and normalized values

- Whitespace-only raw strings become `NULL`; other strings are trimmed.
- `YEAR`, `MONTH`, `DAY_OF_WEEK`, airline ID, and airport/city-market IDs become `IntegerType`.
- `FL_DATE` is parsed with `M/d/yyyy h:mm:ss a` and stored as `DateType`.
- Delay minutes, distance, and delay-cause minutes become `DoubleType`.
- `DEP_DEL15`, `ARR_DEL15`, `CANCELLED`, and `DIVERTED` become `IntegerType` values `0`, `1`, or `NULL`.
- Source flag strings such as `"0.00"` and `"1.00"` are accepted. Other nonblank numeric values are not truncated; they cause validation failure.
- Flight numbers, carrier/airport/tail codes, names, cancellation code, and the four HHmm fields remain strings.
- HHmm values are trimmed only. Leading zeros and literal `2400` are preserved; they are not converted to integers or timestamps.

### Missing values

- Conditional `NULL` values are retained.
- `ARR_DEL15`, cancellation code, and delay-cause fields are not imputed.
- No generic imputation framework is introduced.

### Flight status

| Typed flags | `flight_status` |
|---|---|
| `CANCELLED=0`, `DIVERTED=0` | `completed` |
| `CANCELLED=1`, `DIVERTED=0` | `cancelled` |
| `CANCELLED=0`, `DIVERTED=1` | `diverted` |
| Any other combination | `invalid` |

All valid completed, cancelled, and diverted rows are retained. An `invalid` row makes pre-write validation fail; the pipeline does not silently repair it.

### Duplicates, extremes, and grain

- No `dropDuplicates()` is used because Phase 1 found zero exact duplicates.
- Candidate-key collisions are not removed.
- `ARR_DELAY = 3,407` minutes is retained; no cap, clip, or arbitrary outlier threshold is applied.
- One cleaned row corresponds to one raw row.
- `_source_file` is retained for lineage.
- No surrogate key, star schema, analytical mart, or ML feature is created.

## 4. Verified cleaned schema

The cleaned Parquet read-back contains **38 columns**: the 36 BTS fields plus retained `_source_file` and derived `flight_status`. Runtime schema validation passed.

| Spark type | Columns |
|---|---|
| `DateType` | `FL_DATE` |
| `IntegerType` | `YEAR`, `MONTH`, `DAY_OF_WEEK`, numeric airline/airport IDs, `DEP_DEL15`, `ARR_DEL15`, `CANCELLED`, `DIVERTED` |
| `DoubleType` | `DEP_DELAY`, `ARR_DELAY`, `ARR_DELAY_NEW`, `DISTANCE`, and five delay-cause columns |
| `StringType` | Remaining BTS identifiers/names/codes, four HHmm fields, `_source_file`, `flight_status` |

## 5. Required validation

The pipeline requires every pre-write check to pass before Parquet is created. In the verified run, all **15/15 pre-write checks passed**:

1. Raw count is nonzero and equals cleaned count.
2. Counts by `YEAR`/`MONTH` are unchanged.
3. Raw and cleaned completed/cancelled/diverted/invalid counts match.
4. Status counts reconcile to cleaned rows, and `invalid = 0`.
5. Raw counts, month counts, and status counts match the Phase 1 JSON baseline.
6. Target column names and Spark data types match the contract.
7. No nonblank value becomes `NULL` because of a failed cast or date parse.
8. Binary domains and HHmm formats remain valid.
9. `_source_file` is present and non-null.
10. At row level, completed flights have `ARR_DEL15` in `{0,1}`, while non-completed flights do not carry an `ARR_DEL15` label.

After the write, Spark reads the Parquet directory again and checks total rows, month counts, status counts, schema, lineage, and the `ARR_DEL15` rules against the pre-write DataFrame. Column order and Parquet nullable metadata are not treated as semantic schema differences because partition discovery can change them; names and Spark data types must match.

If a mandatory check fails, the report status is `FAIL` and the job exits with an error. The pipeline does not alter data to make checks pass. If write or read-back fails, it does not delete a partial output directory.

### Verified record reconciliation

| Metric | Raw | Cleaned | Parquet read-back |
|---|---:|---:|---:|
| Total records | 1,645,503 | 1,645,503 | 1,645,503 |
| January 2025 | 539,747 | 539,747 | 539,747 |
| February 2025 | 504,884 | 504,884 | 504,884 |
| March 2025 | 600,872 | 600,872 | 600,872 |

### Verified flight status and quality checks

| Check | Verified result |
|---|---:|
| Completed | 1,611,046 |
| Cancelled | 30,640 |
| Diverted | 3,817 |
| Invalid flight status | 0 |
| Cleaned schema columns | 38 |
| Introduced cast `NULL`s | 0 |
| `ARR_DEL15` row-level violations | 0 |
| Pre-write checks | 15/15 PASS |
| Read-back checks | 9/9 PASS |
| Failed checks | 0 |
| Errors | 0 |

The three valid flight-status counts reconcile to 1,645,503 records. These results validate the implemented Phase 2 rules and the Parquet round trip; they do not prove the dataset is perfect.

## 6. Validation JSON

`artifacts/phase2/validation_summary.json` records the verified run and contains:

- `overall_status` and `failure_stage`.
- Validated input metadata and output configuration.
- Raw, cleaned, and Phase 1 baseline counts.
- Month and flight-status reconciliations.
- Expected/actual type maps and schema mismatches.
- Per-column source nulls, target nulls, and introduced cast nulls.
- Binary and HHmm rule counts.
- Row-level `ARR_DEL15` violation counts and a maximum five-row sample if needed.
- Parquet write and read-back statuses.
- Failed check names, errors, and limitations.

The verified run reached every stage and finished with `overall_status = PASS`. In a failed future run, stages not reached remain `NOT_RUN`; they are not filled with zero.

## 7. Validation conclusion

Phase 2 reproduced the Phase 1 raw/month/status baseline, preserved all 1,645,503 records through cleaning, and read the same count back from Parquet. The cleaned schema contains 38 columns. All 15 pre-write checks and all 9 read-back checks passed, with zero failed checks and zero recorded errors.

This is technical validation of the implemented cleaning and storage rules. Business KPIs, airline/airport/route comparisons, Spark SQL analytical outputs, Power BI measures, and Machine Learning metrics have not been produced.

## 8. Windows Parquet smoke test

The project owner ran `scripts/smoke_test_parquet.py` before the full pipeline. It wrote three synthetic rows with explicit types, including HHmm values `0015` and `2400`, read them back, and validated count and types. The result was **PASS with exit code `0`**. The sample is an environment smoke test, not BTS evidence.

The successful Windows run used Hadoop 3.3.4 Windows utilities. These local environment binaries are not project source and must not be committed. The script continues to refuse an existing smoke-test path and does not delete its output.

## 9. Limitations

- One successful local run verifies Parquet write/read for the stated Windows environment; it is not a production benchmark, SLA, scalability claim, or portability guarantee.
- PASS covers only the implemented rules, not every possible data-quality issue.
- Conditional source nulls are intentionally preserved.
- No duplicate removal, outlier treatment, KPI calculation, Power BI work, feature engineering, or Machine Learning is included.
- Zero introduced cast nulls does not mean the dataset contains no legitimate conditional nulls.
- Zero `ARR_DEL15` violations applies only to the implemented row-level rules.
- Phase 2 is closed, committed and pushed. Phase 3A implementation and runtime validation are complete, pending Git commit; Phase 3B and later phases have not started.

## 10. Interview practice

1. Why are the four HHmm fields stored as strings instead of integers or timestamps?
2. How does the pipeline safely convert source flags such as `"0.00"` without accepting `"0.50"` as zero?
3. Why are cancelled and diverted flights retained in the cleaned dataset, and how should downstream KPI denominators treat them?
4. What are the analytical and performance benefits of Parquet with `YEAR`/`MONTH` partitioning, and what is the small-files trade-off?
5. Why does the pipeline validate both the pre-write DataFrame and a Parquet read-back instead of treating a successful write call as sufficient evidence?
