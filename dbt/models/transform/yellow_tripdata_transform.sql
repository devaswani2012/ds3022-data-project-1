WITH trips AS (
    SELECT *
    FROM yellow_tripdata_clean
),
emissions AS (
    SELECT *
    FROM {{ ref('vehicle_emissions') }}
    WHERE vehicle_type = 'yellow_taxi'
)

SELECT
    t.*,
    (t.trip_distance * e.co2_grams_per_mile) / 1000.0 AS trip_co2_kgs,
    t.trip_distance / NULLIF(EXTRACT(EPOCH FROM (t.tpep_dropoff_datetime - t.tpep_pickup_datetime)) / 3600, 0) AS avg_mph,
    EXTRACT(HOUR FROM t.tpep_pickup_datetime) AS hour_of_day,
    strftime('%A', t.tpep_pickup_datetime) AS day_of_week,
    EXTRACT(WEEK FROM t.tpep_pickup_datetime) AS week_of_year,
    strftime('%B', t.tpep_pickup_datetime) AS month_of_year,
    EXTRACT(YEAR FROM t.tpep_pickup_datetime) AS year
FROM trips t
CROSS JOIN emissions e
