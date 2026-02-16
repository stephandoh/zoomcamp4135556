WITH trips AS (
    SELECT
        pickup_location_id,
        EXTRACT(YEAR FROM pickup_datetime) AS year,
        EXTRACT(MONTH FROM pickup_datetime) AS month,
        taxi_color,
        total_amount
    FROM {{ ref('fct_trips') }}
)

SELECT
    t.pickup_location_id,
    z.zone AS pickup_zone_name,
    t.year,
    t.month,
    t.taxi_color,
    COUNT(*) AS total_monthly_trips,
    SUM(total_amount) AS revenue_monthly_total_amount
FROM trips t
LEFT JOIN {{ ref('dim_zones') }} z
    ON t.pickup_location_id = z.location_id
GROUP BY 1, 2, 3, 4, 5
ORDER BY revenue_monthly_total_amount DESC
