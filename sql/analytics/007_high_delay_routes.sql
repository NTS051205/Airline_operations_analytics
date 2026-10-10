-- Rank observed directed-route delay rates, not predicted risk.
-- WHERE is appropriate: the source already has one row per route/month.
-- Re-GROUP BY / HAVING would add no aggregation at this same grain.
WITH eligible_routes AS (
    SELECT
        YEAR, MONTH, ORIGIN, DEST,
        completed_flights, delayed_arrival_flights, arrival_delay_rate
    FROM route_monthly_performance
    WHERE completed_flights >= :min_completed_flights
      AND arrival_delay_rate IS NOT NULL
), ranked AS (
    SELECT *,
        DENSE_RANK() OVER (
            PARTITION BY YEAR, MONTH ORDER BY arrival_delay_rate DESC
        ) AS observed_delay_rank
    FROM eligible_routes
)
SELECT *
FROM ranked
ORDER BY YEAR, MONTH, observed_delay_rank, ORIGIN, DEST
