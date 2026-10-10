-- BTS weekday coding is retained: 1=Monday, ..., 7=Sunday.
WITH counts AS (
    SELECT YEAR, MONTH, DAY_OF_WEEK,
        COUNT(*) AS total_flights,
        COUNT(CASE WHEN flight_status = 'completed' THEN 1 END) AS completed_flights,
        COUNT(CASE WHEN flight_status = 'completed' AND ARR_DEL15 = 1
                   THEN 1 END) AS delayed_arrival_flights
    FROM flights_cleaned
    GROUP BY YEAR, MONTH, DAY_OF_WEEK
)
SELECT *,
    CAST(delayed_arrival_flights AS DOUBLE)
        / NULLIF(completed_flights, 0) AS arrival_delay_rate
FROM counts
ORDER BY YEAR, MONTH, DAY_OF_WEEK
