-- Shared in-memory summary; only files 001-004 are persisted as outputs.
-- GROUPING distinguishes a subtotal placeholder from a source NULL.
WITH counts AS (
    SELECT
        YEAR, MONTH, OP_UNIQUE_CARRIER, ORIGIN, DEST,
        CASE
            WHEN GROUPING(OP_UNIQUE_CARRIER) = 0 THEN 'airline'
            WHEN GROUPING(DEST) = 0 THEN 'route'
            WHEN GROUPING(ORIGIN) = 0 THEN 'origin_airport'
            ELSE 'monthly'
        END AS aggregation_level,
        COUNT(*) AS total_flights,
        COUNT(CASE WHEN flight_status = 'completed' THEN 1 END) AS completed_flights,
        COUNT(CASE WHEN flight_status = 'cancelled' THEN 1 END) AS cancelled_flights,
        COUNT(CASE WHEN flight_status = 'diverted' THEN 1 END) AS diverted_flights,
        COUNT(CASE WHEN flight_status = 'completed' AND ARR_DEL15 = 1
                   THEN 1 END) AS delayed_arrival_flights,
        COUNT(CASE WHEN flight_status = 'completed' AND ARR_DEL15 = 0
                   THEN 1 END) AS on_time_arrival_flights,
        COUNT(CASE WHEN flight_status = 'completed'
                   THEN ARR_DELAY END) AS arrival_delay_observed_flights,
        SUM(CASE WHEN flight_status = 'completed'
                 THEN ARR_DELAY END) AS arrival_delay_minutes_sum,
        AVG(CASE WHEN flight_status = 'completed'
                 THEN ARR_DELAY END) AS average_arrival_delay
    FROM flights_cleaned
    GROUP BY GROUPING SETS (
        (YEAR, MONTH),
        (YEAR, MONTH, OP_UNIQUE_CARRIER),
        (YEAR, MONTH, ORIGIN),
        (YEAR, MONTH, ORIGIN, DEST)
    )
)
SELECT
    YEAR, MONTH, OP_UNIQUE_CARRIER, ORIGIN, DEST, aggregation_level,
    total_flights, completed_flights, cancelled_flights, diverted_flights,
    delayed_arrival_flights, on_time_arrival_flights,
    arrival_delay_observed_flights, arrival_delay_minutes_sum,
    average_arrival_delay,
    CAST(delayed_arrival_flights AS DOUBLE)
        / NULLIF(completed_flights, 0) AS arrival_delay_rate,
    CAST(on_time_arrival_flights AS DOUBLE)
        / NULLIF(completed_flights, 0) AS on_time_arrival_rate,
    CAST(cancelled_flights AS DOUBLE)
        / NULLIF(total_flights, 0) AS cancellation_rate,
    CAST(diverted_flights AS DOUBLE)
        / NULLIF(total_flights, 0) AS diversion_rate
FROM counts
