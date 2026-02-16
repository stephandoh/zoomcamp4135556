SELECT
    -- identifiers
    CAST(VendorID AS INT64) AS vendor_id,
    CAST(RatecodeID AS FLOAT64) AS ratecode_id,
    CAST(payment_type AS FLOAT64) AS payment_type,
    CAST(trip_type AS FLOAT64) AS trip_type,  -- exists in Green Taxi

    -- timestamps
    CAST(lpep_pickup_datetime AS TIMESTAMP) AS pickup_datetime,
    CAST(lpep_dropoff_datetime AS TIMESTAMP) AS dropoff_datetime,

    -- locations
    CAST(PULocationID AS INT64) AS pickup_location_id,
    CAST(DOLocationID AS INT64) AS dropoff_location_id,

    -- trip info
    CAST(store_and_fwd_flag AS STRING) AS store_and_fwd_flag,
    CAST(passenger_count AS FLOAT64) AS passenger_count,
    CAST(trip_distance AS FLOAT64) AS trip_distance,

    -- payment info
    CAST(fare_amount AS FLOAT64) AS fare_amount,
    CAST(extra AS FLOAT64) AS extra,
    CAST(mta_tax AS FLOAT64) AS mta_tax,
    CAST(tip_amount AS FLOAT64) AS tip_amount,
    CAST(tolls_amount AS FLOAT64) AS tolls_amount,
    CAST(improvement_surcharge AS FLOAT64) AS improvement_surcharge,
    CAST(congestion_surcharge AS FLOAT64) AS congestion_surcharge,
    CAST(total_amount AS FLOAT64) AS total_amount,
    'green' AS taxi_color
FROM {{ source('raw_data', 'green_taxi') }}
WHERE VendorID IS NOT NULL
