-- ==============================================================================
-- 4. GOLD LAYER (Analytics & Business Intelligence)
-- Purpose: Pre-aggregated tables for fast querying and dashboards.
-- ==============================================================================

CREATE SCHEMA IF NOT EXISTS gold;

-- Table for Spatio-Temporal Demand
CREATE TABLE IF NOT EXISTS gold.hourly_spatial_demand (
    pickup_time_id BIGINT,
    pickup_location_id INT,
    taxi_type_id SMALLINT,
    total_departures INT,
    avg_trip_distance NUMERIC(10,2),
    avg_duration_seconds INT,
    PRIMARY KEY (pickup_time_id, pickup_location_id, taxi_type_id)
);

-- Table for Financial & Congestion Analysis
CREATE TABLE IF NOT EXISTS gold.revenue_summary_daily (
    dropoff_date DATE,
    taxi_type_id SMALLINT,
    payment_type_id SMALLINT,
    total_trips INT,
    gross_revenue NUMERIC(15,2),
    total_cbd_fees NUMERIC(15,2),
    total_tips NUMERIC(15,2),
    PRIMARY KEY (dropoff_date, taxi_type_id, payment_type_id)
);



-- EOF