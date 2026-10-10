# BTS Flight Data Dictionary

**Status:** Raw 36-column contract validated in Phase 1; cleaned 38-column Spark schema validated by the successful Phase 2 Parquet read-back.

The January, February, and March 2025 raw CSV files have the same verified 36-column header in the same order. Phase 1 loads every source column as a Spark string so malformed values remain measurable. The logical types below are a validation contract based on BTS definitions and the observed January sample; they are not proof that every row conforms.

## Calendar

| Column | Raw type | Expected logical type | Null expectation | Meaning |
|---|---|---|---|---|
| `YEAR` | string | integer | Expected non-null | Calendar year of the flight record. |
| `MONTH` | string | integer | Expected non-null | Calendar month, expected to match the source file. |
| `DAY_OF_WEEK` | string | integer 1–7 | Expected non-null | BTS day of week, with Monday represented as 1. |
| `FL_DATE` | string | timestamp/date | Expected non-null | Flight date; observed format `M/d/yyyy h:mm:ss a`. |

## Carrier and flight identity

| Column | Raw type | Expected logical type | Null expectation | Meaning |
|---|---|---|---|---|
| `OP_UNIQUE_CARRIER` | string | categorical identifier | Expected non-null | Reporting operating carrier code. |
| `OP_CARRIER_AIRLINE_ID` | string | integer identifier | Expected non-null | Stable DOT airline identifier. |
| `TAIL_NUM` | string | categorical identifier | May be conditionally null | Aircraft tail number; not used as a required duplicate key. |
| `OP_CARRIER_FL_NUM` | string | categorical identifier | Expected non-null | Operating carrier flight number; not treated as a measure. |

## Origin

| Column | Raw type | Expected logical type | Null expectation | Meaning |
|---|---|---|---|---|
| `ORIGIN_AIRPORT_ID` | string | integer identifier | Expected non-null | Stable DOT identifier for the origin airport. |
| `ORIGIN_AIRPORT_SEQ_ID` | string | integer identifier | Expected non-null | Time-specific sequence identifier for the origin airport. |
| `ORIGIN_CITY_MARKET_ID` | string | integer identifier | Expected non-null | Origin city-market identifier. |
| `ORIGIN` | string | categorical identifier | Expected non-null | Origin airport code. |
| `ORIGIN_CITY_NAME` | string | string | Expected non-null | Origin city and state name. |

## Destination

| Column | Raw type | Expected logical type | Null expectation | Meaning |
|---|---|---|---|---|
| `DEST_AIRPORT_ID` | string | integer identifier | Expected non-null | Stable DOT identifier for the destination airport. |
| `DEST_AIRPORT_SEQ_ID` | string | integer identifier | Expected non-null | Time-specific sequence identifier for the destination airport. |
| `DEST_CITY_MARKET_ID` | string | integer identifier | Expected non-null | Destination city-market identifier. |
| `DEST` | string | categorical identifier | Expected non-null | Destination airport code. |
| `DEST_CITY_NAME` | string | string | Expected non-null | Destination city and state name. |

## Departure performance

| Column | Raw type | Expected logical type | Null expectation | Meaning |
|---|---|---|---|---|
| `CRS_DEP_TIME` | string | HHMM string | Expected non-null | Scheduled local departure time; kept as string to preserve leading zeros. |
| `DEP_TIME` | string | HHMM string | Conditionally null | Actual local departure time; normally unavailable for canceled flights. |
| `DEP_DELAY` | string | double | Conditionally null | Signed departure delay in minutes; early departures are negative. |
| `DEP_DEL15` | string | binary numeric flag | Conditionally null | Departure delay of at least 15 minutes (`1`) or not (`0`). |

## Arrival performance

| Column | Raw type | Expected logical type | Null expectation | Meaning |
|---|---|---|---|---|
| `CRS_ARR_TIME` | string | HHMM string | Expected non-null | Scheduled local arrival time; kept as string to preserve leading zeros. |
| `ARR_TIME` | string | HHMM string | Conditionally null | Actual local arrival time; may be unavailable for canceled or diverted flights. |
| `ARR_DELAY` | string | double | Conditionally null | Signed arrival delay in minutes; early arrivals are negative. |
| `ARR_DELAY_NEW` | string | non-negative double | Conditionally null | Arrival delay minutes with early arrivals set to zero. |
| `ARR_DEL15` | string | binary numeric flag | Conditionally null | Arrival delay of at least 15 minutes (`1`) or not (`0`). |

## Operational status

| Column | Raw type | Expected logical type | Null expectation | Meaning |
|---|---|---|---|---|
| `CANCELLED` | string | binary numeric flag | Expected non-null | Canceled-flight indicator. |
| `CANCELLATION_CODE` | string | categorical code | Expected only when canceled | BTS cancellation reason code. |
| `DIVERTED` | string | binary numeric flag | Expected non-null | Diverted-flight indicator. |

## Flight summary

| Column | Raw type | Expected logical type | Null expectation | Meaning |
|---|---|---|---|---|
| `DISTANCE` | string | positive double | Expected non-null | Non-stop distance between origin and destination in miles. |

## Reported delay causes

| Column | Raw type | Expected logical type | Null expectation | Meaning |
|---|---|---|---|---|
| `CARRIER_DELAY` | string | non-negative double | Conditionally populated | Minutes attributed to the carrier. |
| `WEATHER_DELAY` | string | non-negative double | Conditionally populated | Minutes attributed to weather. |
| `NAS_DELAY` | string | non-negative double | Conditionally populated | Minutes attributed to the National Air System. |
| `SECURITY_DELAY` | string | non-negative double | Conditionally populated | Minutes attributed to security. |
| `LATE_AIRCRAFT_DELAY` | string | non-negative double | Conditionally populated | Minutes attributed to a late-arriving aircraft. |

## Profiling-only rules

- Binary flags remain raw strings during ingestion and are checked against numeric values `0` and `1`.
- Scheduled and actual times remain strings; the profiler accepts valid four-character HHMM values and exact `2400`.
- Generic missing percentages are descriptive. Conditional blanks are interpreted using flight status rather than automatically classified as errors.
- Actual-time and delay fields are never eligible pre-departure ML features.
- Logical types are retained after the full profile reported zero cast failures for the validated casts.

## Verified Phase 1 conclusions

- The three monthly files contain 1,645,503 records and the same 36-column schema.
- The profiler reported zero logical-type cast failures.
- The implemented date, HHMM, binary-flag, distance and derived-delay consistency rules reported zero violations.
- `ARR_DEL15` is missing for 34,457 records, numerically matching 30,640 canceled plus 3,817 diverted flights in aggregate. This does not prove row-level alignment; Phase 2 checks the rule row by row.
- `CANCELLATION_CODE`, delay-cause fields and operational actual-time fields remain conditionally nullable. Their nulls are not classified as errors without the relevant flight-status condition.
- `ARR_DELAY` has an observed maximum of 3,407 minutes. The dictionary retains `double` as its logical type; the extreme value is flagged for contextual review rather than automatically rejected.

These conclusions validate the current parsing contract only. They do not prove that every semantic value is correct or that the implemented rules cover every possible data-quality issue.

## Verified Phase 2 cleaned schema

The cleaned dataset contains the 36 BTS source columns plus retained `_source_file` and derived `flight_status`, for **38 columns total**. It was written to `data/processed/flights_cleaned/` as Snappy-compressed Parquet partitioned by `YEAR` and `MONTH`, then read back successfully.

| Spark type | Verified cleaned columns |
|---|---|
| `DateType` | `FL_DATE` |
| `IntegerType` | `YEAR`, `MONTH`, `DAY_OF_WEEK`, `OP_CARRIER_AIRLINE_ID`, `ORIGIN_AIRPORT_ID`, `ORIGIN_AIRPORT_SEQ_ID`, `ORIGIN_CITY_MARKET_ID`, `DEST_AIRPORT_ID`, `DEST_AIRPORT_SEQ_ID`, `DEST_CITY_MARKET_ID`, `DEP_DEL15`, `ARR_DEL15`, `CANCELLED`, `DIVERTED` |
| `DoubleType` | `DEP_DELAY`, `ARR_DELAY`, `ARR_DELAY_NEW`, `DISTANCE`, `CARRIER_DELAY`, `WEATHER_DELAY`, `NAS_DELAY`, `SECURITY_DELAY`, `LATE_AIRCRAFT_DELAY` |
| `StringType` | All remaining BTS identifiers, codes, city names and HHmm fields, plus `_source_file` and `flight_status` |

Phase 2 recorded zero introduced cast nulls and zero violations of the implemented row-level `ARR_DEL15` rules. Conditional source nulls remain valid where allowed; these results do not imply that every field is non-null or that the dataset is perfect.
