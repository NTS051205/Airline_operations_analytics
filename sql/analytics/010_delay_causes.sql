-- Minute shares among completed flights with the official ARR_DEL15=1 label.
-- No COALESCE(cause_minutes, 0): an unreported cause is not a verified zero.
WITH eligible AS (
    SELECT YEAR, MONTH,
        CARRIER_DELAY, WEATHER_DELAY, NAS_DELAY, SECURITY_DELAY, LATE_AIRCRAFT_DELAY,
        (CARRIER_DELAY IS NOT NULL OR WEATHER_DELAY IS NOT NULL
         OR NAS_DELAY IS NOT NULL OR SECURITY_DELAY IS NOT NULL
         OR LATE_AIRCRAFT_DELAY IS NOT NULL) AS any_cause_reported,
        (CARRIER_DELAY IS NOT NULL AND WEATHER_DELAY IS NOT NULL
         AND NAS_DELAY IS NOT NULL AND SECURITY_DELAY IS NOT NULL
         AND LATE_AIRCRAFT_DELAY IS NOT NULL) AS all_causes_reported
    FROM flights_cleaned
    WHERE flight_status = 'completed' AND ARR_DEL15 = 1
), expanded AS (
    SELECT YEAR, MONTH, any_cause_reported, all_causes_reported,
        STACK(5,
            'CARRIER_DELAY', CARRIER_DELAY,
            'WEATHER_DELAY', WEATHER_DELAY,
            'NAS_DELAY', NAS_DELAY,
            'SECURITY_DELAY', SECURITY_DELAY,
            'LATE_AIRCRAFT_DELAY', LATE_AIRCRAFT_DELAY
        ) AS (cause_category, cause_minutes)
    FROM eligible
), category_totals AS (
    SELECT YEAR, MONTH, cause_category,
        COUNT(*) AS eligible_delayed_flights,
        COUNT(cause_minutes) AS reported_cause_flights,
        COUNT(CASE WHEN any_cause_reported THEN 1 END) AS flights_with_any_reported_cause,
        COUNT(CASE WHEN all_causes_reported THEN 1 END) AS flights_with_all_reported_causes,
        SUM(cause_minutes) AS attributed_minutes
    FROM expanded
    GROUP BY YEAR, MONTH, cause_category
), denominators AS (
    SELECT *,
        SUM(attributed_minutes) OVER (PARTITION BY YEAR, MONTH)
            AS monthly_attributed_minutes
    FROM category_totals
)
SELECT *,
    attributed_minutes / NULLIF(monthly_attributed_minutes, 0.0) AS cause_minute_share
FROM denominators
ORDER BY YEAR, MONTH, cause_minute_share DESC NULLS LAST, cause_category
