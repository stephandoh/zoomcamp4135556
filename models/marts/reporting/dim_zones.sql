WITH taxi_zone_lookup AS (
    SELECT * FROM {{ ref('taxi_zone_lookup') }}
),
renamed AS(
    SELECT
        LocationID AS location_id,
        Borough AS borough,
        Zone AS zone,
        service_zone AS service_zone
    FROM taxi_zone_lookup
)

SELECT * FROM renamed