SELECT
    pickup_zone_name,
    SUM(revenue_monthly_total_amount) AS total_revenue
FROM {{ ref('fct_monthly_zone_revenue') }}
WHERE taxi_color = 'green'
  AND EXTRACT(YEAR FROM pickup_datetime) = 2020
GROUP BY pickup_zone_name
ORDER BY total_revenue DESC
LIMIT 1;
