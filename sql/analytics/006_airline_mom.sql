-- Rates are fractions (0-1); pp difference and relative % change are distinct.
WITH previous_observation AS (
    SELECT
        YEAR, MONTH, OP_UNIQUE_CARRIER,
        completed_flights, delayed_arrival_flights, arrival_delay_rate,
        MAKE_DATE(YEAR, MONTH, 1) AS month_start,
        LAG(MAKE_DATE(YEAR, MONTH, 1)) OVER carrier_month AS previous_month,
        LAG(completed_flights) OVER carrier_month AS previous_completed_flights,
        LAG(delayed_arrival_flights) OVER carrier_month AS previous_delayed_flights,
        LAG(arrival_delay_rate) OVER carrier_month AS previous_delay_rate
    FROM airline_monthly_performance
    WINDOW carrier_month AS (
        PARTITION BY OP_UNIQUE_CARRIER ORDER BY YEAR, MONTH
    )
), comparison AS (
    SELECT *,
        COALESCE(previous_month = ADD_MONTHS(month_start, -1), FALSE)
            AS has_previous_calendar_month
    FROM previous_observation
)
SELECT
    YEAR, MONTH, OP_UNIQUE_CARRIER,
    completed_flights, delayed_arrival_flights, arrival_delay_rate,
    previous_month, previous_completed_flights,
    previous_delayed_flights, previous_delay_rate,
    has_previous_calendar_month,
    CASE WHEN has_previous_calendar_month
         THEN 100.0 * (arrival_delay_rate - previous_delay_rate)
    END AS delay_rate_change_pp,
    CASE WHEN has_previous_calendar_month AND previous_delay_rate > 0
         THEN 100.0 * (arrival_delay_rate - previous_delay_rate)
                    / NULLIF(previous_delay_rate, 0.0)
    END AS delay_rate_relative_change_pct,
    COALESCE(
        has_previous_calendar_month
        AND completed_flights >= :min_completed_flights
        AND previous_completed_flights >= :min_completed_flights,
        FALSE
    ) AS both_months_meet_volume_heuristic
FROM comparison
ORDER BY OP_UNIQUE_CARRIER, YEAR, MONTH
