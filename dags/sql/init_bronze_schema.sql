-- ==============================================================================
-- BRONZE LAYER
-- Purpose: 1:1 ingestion from source. No strict constraints.
-- ==============================================================================

CREATE SCHEMA IF NOT EXISTS bronze;

DROP TABLE IF EXISTS bronze.yellow_trips;
DROP TABLE IF EXISTS bronze.green_trips;
DROP TABLE IF EXISTS bronze.zone_lookup;

CREATE TABLE IF NOT EXISTS bronze.yellow_trips (
    "VendorID" INT,
    tpep_pickup_datetime TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,
    passenger_count BIGINT,
    trip_distance DOUBLE PRECISION,
    "RatecodeID" BIGINT,
    store_and_fwd_flag TEXT,
    "PULocationID" INT,
    "DOLocationID" INT,
    payment_type BIGINT,
    fare_amount DOUBLE PRECISION,
    extra DOUBLE PRECISION,
    mta_tax DOUBLE PRECISION,
    tip_amount DOUBLE PRECISION,
    tolls_amount DOUBLE PRECISION,
    improvement_surcharge DOUBLE PRECISION,
    total_amount DOUBLE PRECISION,
    congestion_surcharge DOUBLE PRECISION,
    "Airport_fee" DOUBLE PRECISION,
    cbd_congestion_fee DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS bronze.green_trips (
    "VendorID" INT,
    lpep_pickup_datetime TIMESTAMP,
    lpep_dropoff_datetime TIMESTAMP,
    store_and_fwd_flag TEXT,
    "RatecodeID" BIGINT,
    "PULocationID" INT,
    "DOLocationID" INT,
    passenger_count BIGINT,
    trip_distance DOUBLE PRECISION,
    fare_amount DOUBLE PRECISION,
    extra DOUBLE PRECISION,
    mta_tax DOUBLE PRECISION,
    tip_amount DOUBLE PRECISION,
    tolls_amount DOUBLE PRECISION,
    ehail_fee DOUBLE PRECISION,
    improvement_surcharge DOUBLE PRECISION,
    total_amount DOUBLE PRECISION,
    payment_type BIGINT,
    trip_type BIGINT,
    congestion_surcharge DOUBLE PRECISION,
    cbd_congestion_fee DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS bronze.zone_lookup (
    "LocationID" BIGINT,
    "Borough" TEXT,
    "Zone" TEXT,
    service_zone TEXT
);



-- EOF