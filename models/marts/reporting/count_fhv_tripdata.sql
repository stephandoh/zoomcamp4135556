SELECT
    COUNT(*) AS total_records
FROM {{ ref('stg_fhv_tripdata') }}