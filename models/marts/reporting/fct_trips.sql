with trips AS (
    SELECT *
    FROM {{ ref('int_trips_unioned') }}
)
SELECT * FROM trips