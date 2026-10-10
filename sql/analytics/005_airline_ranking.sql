-- :min_completed_flights is a positive integer; default 30, a heuristic only.
-- Keep low-volume groups visible but leave their comparison rank NULL.
WITH eligible AS (
    SELECT
        YEAR, MONTH, OP_UNIQUE_CARRIER,
        DENSE_RANK() OVER (
            PARTITION BY YEAR, MONTH
            ORDER BY arrival_delay_rate ASC
        ) AS arrival_delay_rank
    FROM airline_monthly_performance
    WHERE completed_flights >= :min_completed_flights
      AND arrival_delay_rate IS NOT NULL
)
SELECT
    a.YEAR, a.MONTH, a.OP_UNIQUE_CARRIER,
    a.completed_flights, a.delayed_arrival_flights, a.arrival_delay_rate,
    a.completed_flights >= :min_completed_flights AS meets_volume_heuristic,
    e.arrival_delay_rank
FROM airline_monthly_performance a
LEFT JOIN eligible e
    ON a.YEAR = e.YEAR AND a.MONTH = e.MONTH
   AND a.OP_UNIQUE_CARRIER = e.OP_UNIQUE_CARRIER
ORDER BY a.YEAR, a.MONTH, e.arrival_delay_rank ASC NULLS LAST,
         a.OP_UNIQUE_CARRIER
