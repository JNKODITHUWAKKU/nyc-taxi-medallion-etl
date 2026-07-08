-- ==============================================================================
-- 1. POPULATE DIM_ZONE
-- Upsert new zones dynamically from the bronze lookup table.
-- ==============================================================================
INSERT INTO silver.dim_zone (location_id, borough, zone_name, service_zone)
SELECT "LocationID", "Borough", "Zone", service_zone
FROM bronze.zone_lookup
ON CONFLICT (location_id) DO UPDATE 
SET borough = EXCLUDED.borough,
    zone_name = EXCLUDED.zone_name,
    service_zone = EXCLUDED.service_zone;

-- ==============================================================================
-- 2. POPULATE DIM_TIME (Transformation 1: Date Formatting & Extraction)
-- Extracts all unique pickup/dropoff hours from both datasets to build the dimension.
-- ==============================================================================
INSERT INTO silver.dim_time (time_id, datetime_ts, hour, day_of_week, is_weekend)
WITH combined_timestamps AS (
    SELECT tpep_pickup_datetime AS ts FROM bronze.yellow_trips
    UNION
    SELECT tpep_dropoff_datetime AS ts FROM bronze.yellow_trips
    UNION
    SELECT lpep_pickup_datetime AS ts FROM bronze.green_trips
    UNION
    SELECT lpep_dropoff_datetime AS ts FROM bronze.green_trips
)
SELECT DISTINCT
    CAST(TO_CHAR(ts, 'YYYYMMDDHH24') AS BIGINT) AS time_id,
    DATE_TRUNC('hour', ts) AS datetime_ts,
    CAST(EXTRACT(HOUR FROM ts) AS SMALLINT) AS hour,
    CAST(EXTRACT(ISODOW FROM ts) AS SMALLINT) AS day_of_week, -- 1=Monday, 7=Sunday
    CASE WHEN EXTRACT(ISODOW FROM ts) IN (6, 7) THEN TRUE ELSE FALSE END AS is_weekend
FROM combined_timestamps
WHERE ts IS NOT NULL
ON CONFLICT (time_id) DO NOTHING;

-- ==============================================================================
-- 3. POPULATE FACT TABLE (Transformation 2 & 3: Unification & Derived Metrics)
-- Cleans data, aligns schemas using NULL casts, and computes duration.
-- ==============================================================================

-- Ensure idempotency: If the DAG is rerun, we don't duplicate the fact records. 
-- Assuming Bronze holds the current processing batch, we truncate before loading.
TRUNCATE TABLE silver.fact_taxi_trips;

INSERT INTO silver.fact_taxi_trips (
    taxi_type_id, vendor_id, pickup_time_id, dropoff_time_id, 
    pickup_location_id, dropoff_location_id, rate_code_id, payment_type_id, 
    trip_type, passenger_count, trip_distance, trip_duration_seconds, 
    fare_amount, extra, mta_tax, tip_amount, tolls_amount, ehail_fee, 
    improvement_surcharge, congestion_surcharge, cbd_congestion_fee, 
    airport_fee, total_amount
)
WITH unified_trips AS (
    
    -- ---------------------------------------------------------
    -- YELLOW TAXIS
    -- ---------------------------------------------------------
    SELECT 
        1::SMALLINT AS taxi_type_id,
        CAST("VendorID" AS SMALLINT) AS vendor_id,
        CAST(TO_CHAR(tpep_pickup_datetime, 'YYYYMMDDHH24') AS BIGINT) AS pickup_time_id,
        CAST(TO_CHAR(tpep_dropoff_datetime, 'YYYYMMDDHH24') AS BIGINT) AS dropoff_time_id,
        "PULocationID" AS pickup_location_id,
        "DOLocationID" AS dropoff_location_id,
        CAST("RatecodeID" AS SMALLINT) AS rate_code_id,
        CAST(payment_type AS SMALLINT) AS payment_type_id,
        
        NULL::SMALLINT AS trip_type, -- Missing in Yellow
        
        CAST(passenger_count AS SMALLINT) AS passenger_count,
        CAST(trip_distance AS NUMERIC(10,2)) AS trip_distance,
        CAST(EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) AS INT) AS trip_duration_seconds,
        
        CAST(fare_amount AS NUMERIC(10,2)) AS fare_amount,
        CAST(extra AS NUMERIC(10,2)) AS extra,
        CAST(mta_tax AS NUMERIC(10,2)) AS mta_tax,
        CAST(tip_amount AS NUMERIC(10,2)) AS tip_amount,
        CAST(tolls_amount AS NUMERIC(10,2)) AS tolls_amount,
        
        NULL::NUMERIC(10,2) AS ehail_fee, -- Missing in Yellow
        
        CAST(improvement_surcharge AS NUMERIC(10,2)) AS improvement_surcharge,
        CAST(congestion_surcharge AS NUMERIC(10,2)) AS congestion_surcharge,
        CAST(cbd_congestion_fee AS NUMERIC(10,2)) AS cbd_congestion_fee,
        CAST("Airport_fee" AS NUMERIC(10,2)) AS airport_fee,
        CAST(total_amount AS NUMERIC(10,2)) AS total_amount
    FROM bronze.yellow_trips
    WHERE tpep_pickup_datetime IS NOT NULL 
      AND tpep_dropoff_datetime IS NOT NULL
      AND trip_distance >= 0 -- Data Quality: Remove invalid negative distances

    UNION ALL

    -- ---------------------------------------------------------
    -- GREEN TAXIS
    -- ---------------------------------------------------------
    SELECT 
        2::SMALLINT AS taxi_type_id,
        CAST("VendorID" AS SMALLINT) AS vendor_id,
        CAST(TO_CHAR(lpep_pickup_datetime, 'YYYYMMDDHH24') AS BIGINT) AS pickup_time_id,
        CAST(TO_CHAR(lpep_dropoff_datetime, 'YYYYMMDDHH24') AS BIGINT) AS dropoff_time_id,
        "PULocationID" AS pickup_location_id,
        "DOLocationID" AS dropoff_location_id,
        CAST("RatecodeID" AS SMALLINT) AS rate_code_id,
        CAST(payment_type AS SMALLINT) AS payment_type_id,
        
        CAST(trip_type AS SMALLINT) AS trip_type,
        
        CAST(passenger_count AS SMALLINT) AS passenger_count,
        CAST(trip_distance AS NUMERIC(10,2)) AS trip_distance,
        CAST(EXTRACT(EPOCH FROM (lpep_dropoff_datetime - lpep_pickup_datetime)) AS INT) AS trip_duration_seconds,
        
        CAST(fare_amount AS NUMERIC(10,2)) AS fare_amount,
        CAST(extra AS NUMERIC(10,2)) AS extra,
        CAST(mta_tax AS NUMERIC(10,2)) AS mta_tax,
        CAST(tip_amount AS NUMERIC(10,2)) AS tip_amount,
        CAST(tolls_amount AS NUMERIC(10,2)) AS tolls_amount,
        CAST(ehail_fee AS NUMERIC(10,2)) AS ehail_fee,
        
        CAST(improvement_surcharge AS NUMERIC(10,2)) AS improvement_surcharge,
        CAST(congestion_surcharge AS NUMERIC(10,2)) AS congestion_surcharge,
        CAST(cbd_congestion_fee AS NUMERIC(10,2)) AS cbd_congestion_fee,
        NULL::NUMERIC(10,2) AS airport_fee, -- Missing in Green
        CAST(total_amount AS NUMERIC(10,2)) AS total_amount
    FROM bronze.green_trips
    WHERE lpep_pickup_datetime IS NOT NULL 
      AND lpep_dropoff_datetime IS NOT NULL
      AND trip_distance >= 0 -- Data Quality: Remove invalid negative distances
)
SELECT * FROM unified_trips;