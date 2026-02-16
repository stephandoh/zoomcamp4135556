WITH trips_unioned AS (
    SELECT * FROM {{ ref('int_trips_unioned') }}
),
vendors AS (
    SELECT DISTINCT vendor_id FROM trips_unioned
)   

SELECT * FROM vendors
