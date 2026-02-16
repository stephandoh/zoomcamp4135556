SELECT dz.zone AS pickup_zone,
       SUM(fm.revenue_monthly_total_amount) AS total_revenue
FROM {{ ref('fct_monthly_zone_revenue') }} AS fm
JOIN {{ ref('dim_zones') }} AS dz
  ON fm.pickup_location_id = dz.location_id
WHERE fm.taxi_color = 'green'
  AND fm.year = 2020        -- filter by year column
GROUP BY dz.zone
ORDER BY total_revenue DESC
LIMIT 1;
