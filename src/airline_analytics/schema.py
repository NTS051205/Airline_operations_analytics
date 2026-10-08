"""Confirmed Phase 1 schema contract for the BTS raw CSV files."""

EXPECTED_YEAR = 2025
EXPECTED_MONTHS = (1, 2, 3)
FLIGHT_DATE_PATTERN = "M/d/yyyy h:mm:ss a"
VALID_HHMM_PATTERN = r"^(?:(?:[01]\d|2[0-3])[0-5]\d|2400)$"

EXPECTED_COLUMNS = (
    "YEAR",
    "MONTH",
    "DAY_OF_WEEK",
    "FL_DATE",
    "OP_UNIQUE_CARRIER",
    "OP_CARRIER_AIRLINE_ID",
    "TAIL_NUM",
    "OP_CARRIER_FL_NUM",
    "ORIGIN_AIRPORT_ID",
    "ORIGIN_AIRPORT_SEQ_ID",
    "ORIGIN_CITY_MARKET_ID",
    "ORIGIN",
    "ORIGIN_CITY_NAME",
    "DEST_AIRPORT_ID",
    "DEST_AIRPORT_SEQ_ID",
    "DEST_CITY_MARKET_ID",
    "DEST",
    "DEST_CITY_NAME",
    "CRS_DEP_TIME",
    "DEP_TIME",
    "DEP_DELAY",
    "DEP_DEL15",
    "CRS_ARR_TIME",
    "ARR_TIME",
    "ARR_DELAY",
    "ARR_DELAY_NEW",
    "ARR_DEL15",
    "CANCELLED",
    "CANCELLATION_CODE",
    "DIVERTED",
    "DISTANCE",
    "CARRIER_DELAY",
    "WEATHER_DELAY",
    "NAS_DELAY",
    "SECURITY_DELAY",
    "LATE_AIRCRAFT_DELAY",
)

INTEGER_COLUMNS = (
    "YEAR",
    "MONTH",
    "DAY_OF_WEEK",
    "OP_CARRIER_AIRLINE_ID",
    "ORIGIN_AIRPORT_ID",
    "ORIGIN_AIRPORT_SEQ_ID",
    "ORIGIN_CITY_MARKET_ID",
    "DEST_AIRPORT_ID",
    "DEST_AIRPORT_SEQ_ID",
    "DEST_CITY_MARKET_ID",
)

DOUBLE_COLUMNS = (
    "DEP_DELAY",
    "DEP_DEL15",
    "ARR_DELAY",
    "ARR_DELAY_NEW",
    "ARR_DEL15",
    "CANCELLED",
    "DIVERTED",
    "DISTANCE",
    "CARRIER_DELAY",
    "WEATHER_DELAY",
    "NAS_DELAY",
    "SECURITY_DELAY",
    "LATE_AIRCRAFT_DELAY",
)

TIME_COLUMNS = (
    "CRS_DEP_TIME",
    "DEP_TIME",
    "CRS_ARR_TIME",
    "ARR_TIME",
)

BINARY_FLAG_COLUMNS = (
    "DEP_DEL15",
    "ARR_DEL15",
    "CANCELLED",
    "DIVERTED",
)

DELAY_CAUSE_COLUMNS = (
    "CARRIER_DELAY",
    "WEATHER_DELAY",
    "NAS_DELAY",
    "SECURITY_DELAY",
    "LATE_AIRCRAFT_DELAY",
)

# This is a candidate key for profiling, not a declared primary key.
BUSINESS_KEY_COLUMNS = (
    "FL_DATE",
    "OP_CARRIER_AIRLINE_ID",
    "OP_CARRIER_FL_NUM",
    "ORIGIN_AIRPORT_ID",
    "DEST_AIRPORT_ID",
    "CRS_DEP_TIME",
)

LOGICAL_TYPES = {
    **{column: "integer" for column in INTEGER_COLUMNS},
    **{column: "double" for column in DOUBLE_COLUMNS},
    "FL_DATE": "timestamp/date",
    "OP_UNIQUE_CARRIER": "string identifier",
    "TAIL_NUM": "string identifier",
    "OP_CARRIER_FL_NUM": "string identifier",
    "ORIGIN": "string identifier",
    "ORIGIN_CITY_NAME": "string",
    "DEST": "string identifier",
    "DEST_CITY_NAME": "string",
    "CRS_DEP_TIME": "HHMM string",
    "DEP_TIME": "HHMM string",
    "CRS_ARR_TIME": "HHMM string",
    "ARR_TIME": "HHMM string",
    "CANCELLATION_CODE": "string code",
}

EXPECTED_FILE_METADATA = {
    "reporting_carrier_ontime_2025_01.csv": {
        "size_bytes": 111_474_672,
        "sha256": "26041E8542BCBD25226A5013718FEEF400F1937C8F0471725B55B1E7B665CFCB",
    },
    "reporting_carrier_ontime_2025_02.csv": {
        "size_bytes": 104_767_501,
        "sha256": "E0A24E7F61237C5F56D02517D31387A25397D38C7F8E980DD84697E0E69ECE02",
    },
    "reporting_carrier_ontime_2025_03.csv": {
        "size_bytes": 124_595_215,
        "sha256": "1F0FEA2588A633F37B64C18BFCC44A2CF915FE3EC6C24EE698C93986ED149EF3",
    },
}
