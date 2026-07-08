-- ==============================================================================
-- SILVER LAYER (Star Schema Dimensions)
-- Purpose: Normalized reference tables with strict Primary Keys.
-- ==============================================================================

CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver.dim_taxi_type (
    taxi_type_id SMALLINT PRIMARY KEY,
    taxi_type_name VARCHAR(10) NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.dim_vendor (
    vendor_id SMALLINT PRIMARY KEY,
    vendor_name VARCHAR(50) NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.dim_rate_code (
    rate_code_id SMALLINT PRIMARY KEY,
    rate_description VARCHAR(50) NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.dim_payment_type (
    payment_type_id SMALLINT PRIMARY KEY,
    payment_description VARCHAR(50) NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.dim_zone (
    location_id INT PRIMARY KEY,
    borough VARCHAR(50),
    zone_name VARCHAR(100),
    service_zone VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS silver.dim_time (
    time_id BIGINT PRIMARY KEY, -- Format: YYYYMMDDHH
    datetime_ts TIMESTAMP NOT NULL,
    hour SMALLINT NOT NULL,
    day_of_week SMALLINT NOT NULL,
    is_weekend BOOLEAN NOT NULL
);

-- Pre-populate static dimensions based on the Data Dictionary
INSERT INTO silver.dim_taxi_type (taxi_type_id, taxi_type_name) 
VALUES (1, 'Yellow'), (2, 'Green') 
ON CONFLICT (taxi_type_id) DO NOTHING;

INSERT INTO silver.dim_vendor (vendor_id, vendor_name) 
VALUES (1, 'Creative Mobile Technologies'), (2, 'Curb Mobility'), (6, 'Myle Technologies'), (7, 'Helix') 
ON CONFLICT (vendor_id) DO NOTHING;

INSERT INTO silver.dim_rate_code (rate_code_id, rate_description) 
VALUES (1, 'Standard rate'), (2, 'JFK'), (3, 'Newark'), (4, 'Nassau or Westchester'), (5, 'Negotiated fare'), (6, 'Group ride'), (99, 'Null/unknown') 
ON CONFLICT (rate_code_id) DO NOTHING;

INSERT INTO silver.dim_payment_type (payment_type_id, payment_description) 
VALUES (0, 'Flex Fare'), (1, 'Credit card'), (2, 'Cash'), (3, 'No charge'), (4, 'Dispute'), (5, 'Unknown'), (6, 'Voided trip') 
ON CONFLICT (payment_type_id) DO NOTHING;

-- ==============================================================================
-- 3. SILVER LAYER (Star Schema Fact Table)
-- Purpose: Central transactional data with strict Foreign Keys.
-- ==============================================================================

CREATE TABLE IF NOT EXISTS silver.fact_taxi_trips (
    trip_id BIGSERIAL PRIMARY KEY,
    taxi_type_id SMALLINT REFERENCES silver.dim_taxi_type(taxi_type_id),
    vendor_id SMALLINT REFERENCES silver.dim_vendor(vendor_id),
    pickup_time_id BIGINT REFERENCES silver.dim_time(time_id),
    dropoff_time_id BIGINT REFERENCES silver.dim_time(time_id),
    pickup_location_id INT REFERENCES silver.dim_zone(location_id),
    dropoff_location_id INT REFERENCES silver.dim_zone(location_id),
    rate_code_id SMALLINT REFERENCES silver.dim_rate_code(rate_code_id),
    payment_type_id SMALLINT REFERENCES silver.dim_payment_type(payment_type_id),
    trip_type SMALLINT, -- Green only (1=Street-hail, 2=Dispatch)
    
    passenger_count SMALLINT,
    trip_distance NUMERIC(10,2),
    trip_duration_seconds INT,
    
    fare_amount NUMERIC(10,2),
    extra NUMERIC(10,2),
    mta_tax NUMERIC(10,2),
    tip_amount NUMERIC(10,2),
    tolls_amount NUMERIC(10,2),
    ehail_fee NUMERIC(10,2), -- Green only
    improvement_surcharge NUMERIC(10,2),
    congestion_surcharge NUMERIC(10,2),
    cbd_congestion_fee NUMERIC(10,2),
    airport_fee NUMERIC(10,2), -- Yellow only
    total_amount NUMERIC(10,2)
);

-- Add indexes on Foreign Keys to heavily optimize analytical JOIN operations
CREATE INDEX IF NOT EXISTS idx_fact_pickup_time ON silver.fact_taxi_trips(pickup_time_id);
CREATE INDEX IF NOT EXISTS idx_fact_pickup_loc ON silver.fact_taxi_trips(pickup_location_id);
CREATE INDEX IF NOT EXISTS idx_fact_taxi_type ON silver.fact_taxi_trips(taxi_type_id);



-- EOF