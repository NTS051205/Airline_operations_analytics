"""Confirmed Phase 1 schema contract for the BTS raw CSV files."""

EXPECTED_YEAR = 2025
EXPECTED_MONTHS = tuple(range(1, 13))
DATASET_SCOPE = "BTS Reporting Carrier On-Time Performance, 2025-01 to 2025-12"
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
    # Local file fingerprints verified before annual profiling; not BTS checksums.
    "reporting_carrier_ontime_2025_04.csv": {
        "size_bytes": 121_117_142,
        "sha256": "C6A7520063E38906749710AC87C6E744F758C19129897243A2B0E96FE3198164",
    },
    "reporting_carrier_ontime_2025_05.csv": {
        "size_bytes": 126_073_022,
        "sha256": "5C850A91F9854C9D9DA281633C589D373D63BE9683FEF754F45B3E02B5CF02CB",
    },
    "reporting_carrier_ontime_2025_06.csv": {
        "size_bytes": 127_791_220,
        "sha256": "AA1E584A3610C13455C042886555D2D3382B15EF106F53B1BE0B535A64158B0A",
    },
    "reporting_carrier_ontime_2025_07.csv": {
        "size_bytes": 131_835_656,
        "sha256": "3A29CE60DD9BF8EAC320E3BE93203C5A9122935669251B37B875C1D753901BBB",
    },
    "reporting_carrier_ontime_2025_08.csv": {
        "size_bytes": 125_210_254,
        "sha256": "76A0376ED71F4B363442D99F0FE0B005489FD60C8B792D2AD304DE15581EF873",
    },
    "reporting_carrier_ontime_2025_09.csv": {
        "size_bytes": 116_310_797,
        "sha256": "50535E0D4D7400303D5BE100FD8E69139C601A05FBDBF995F9A7CD67A93808C3",
    },
    "reporting_carrier_ontime_2025_10.csv": {
        "size_bytes": 126_978_033,
        "sha256": "92F9C2280D4626DCE2AA04A6224B4E46BC97FA84971BC1B81FD18AA905AC6D63",
    },
    "reporting_carrier_ontime_2025_11.csv": {
        "size_bytes": 119_308_511,
        "sha256": "3F98FFE7ECFCFA3F3E2B511174F374B5E8202E4E8C8BC31D9BC42FDBBD2E4CDF",
    },
    "reporting_carrier_ontime_2025_12.csv": {
        "size_bytes": 122_713_243,
        "sha256": "602F94C9B2098EC35CF4B58DD066D83AAD742708EFDB2C185632D104D1BD66CC",
    },
}
