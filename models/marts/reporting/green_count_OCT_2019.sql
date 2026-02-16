SELECT 
    SUM(total_monthly_trips) AS total_green_taxi_trips_oct2019
FROM {{ ref('fct_monthly_zone_revenue') }}
WHERE taxi_color = 'green'
  AND year = 2019
  AND month = 10;
