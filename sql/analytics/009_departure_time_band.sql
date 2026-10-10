-- Compare four-character HHmm strings only after checking their format.
-- 2400 maps to Night; this grouping does not change the flight date/month.
WITH banded AS (
    SELECT YEAR, MONTH, flight_status, ARR_DEL15, CRS_DEP_TIME,
        CASE
            WHEN CRS_DEP_TIME IS NULL THEN 'Missing'
            WHEN CRS_DEP_TIME = '2400' THEN 'Night'
            WHEN NOT (CRS_DEP_TIME RLIKE '^([01][0-9]|2[0-3])[0-5][0-9]$')
                THEN 'Invalid'
            WHEN CRS_DEP_TIME < '0600' THEN 'Night'
            WHEN CRS_DEP_TIME < '1200' THEN 'Morning'
            WHEN CRS_DEP_TIME < '1800' THEN 'Afternoon'
            ELSE 'Evening'
        END AS time_band
    FROM flights_cleaned
), counts AS (
    SELECT YEAR, MONTH, time_band,
        COUNT(*) AS total_flights,
        COUNT(CASE WHEN flight_status = 'completed' THEN 1 END) AS completed_flights,
        COUNT(CASE WHEN flight_status = 'completed' AND ARR_DEL15 = 1
                   THEN 1 END) AS delayed_arrival_flights,
        COUNT(CASE WHEN CRS_DEP_TIME = '2400' THEN 1 END) AS scheduled_2400_flights
    FROM banded
    GROUP BY YEAR, MONTH, time_band
)
SELECT *,
    CAST(delayed_arrival_flights AS DOUBLE)
        / NULLIF(completed_flights, 0) AS arrival_delay_rate
FROM counts
ORDER BY YEAR, MONTH,
    CASE time_band WHEN 'Night' THEN 1 WHEN 'Morning' THEN 2
        WHEN 'Afternoon' THEN 3 WHEN 'Evening' THEN 4
        WHEN 'Missing' THEN 5 ELSE 6 END
