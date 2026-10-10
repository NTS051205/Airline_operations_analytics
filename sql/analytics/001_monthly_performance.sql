-- One row per calendar month; no airline/airport/route subtotals included.
SELECT
    YEAR, MONTH,
    total_flights, completed_flights, cancelled_flights, diverted_flights,
    delayed_arrival_flights, on_time_arrival_flights,
    arrival_delay_observed_flights, arrival_delay_minutes_sum,
    average_arrival_delay,
    arrival_delay_rate, on_time_arrival_rate, cancellation_rate, diversion_rate
FROM kpi_summary
WHERE aggregation_level = 'monthly'
