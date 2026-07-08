-- ==============================================================================
-- 1. Fleet Rebalancing: Net Spatial Flow
-- Business Insight: Identifies which zones are "draining" vehicles (more pickups 
-- than dropoffs) versus "accumulating" vehicles, which is critical for logistics 
-- companies to dispatch drivers for rebalancing.
-- ==============================================================================

WITH pickups AS (
    SELECT 
        pickup_location_id AS location_id,
        COUNT(*) AS total_departures
    FROM silver.fact_taxi_trips
    GROUP BY pickup_location_id
),
dropoffs AS (
    SELECT 
        dropoff_location_id AS location_id,
        COUNT(*) AS total_arrivals
    FROM silver.fact_taxi_trips
    GROUP BY dropoff_location_id
)
SELECT 
    z.borough,
    z.zone_name,
    COALESCE(p.total_departures, 0) AS departures,
    COALESCE(d.total_arrivals, 0) AS arrivals,
    COALESCE(d.total_arrivals, 0) - COALESCE(p.total_departures, 0) AS net_fleet_flow
FROM silver.dim_zone z
LEFT JOIN pickups p ON z.location_id = p.location_id
LEFT JOIN dropoffs d ON z.location_id = d.location_id
ORDER BY net_fleet_flow ASC
LIMIT 15; -- Shows the top 15 highest deficit zones


-- ==============================================================================
-- 2. Policy Impact: CBD Congestion Fee Revenue
-- Business Insight: Evaluates the financial impact of the newly implemented 
-- Congestion Relief Zone fee, calculating what percentage of gross revenue it 
-- constitutes across different boroughs.
-- ==============================================================================

SELECT 
    z.borough,
    t.taxi_type_name,
    COUNT(f.trip_id) AS total_trips,
    SUM(f.total_amount) AS gross_revenue,
    SUM(f.cbd_congestion_fee) AS total_congestion_fees_collected,
    ROUND(
        (SUM(f.cbd_congestion_fee) / NULLIF(SUM(f.total_amount), 0)) * 100, 
        2
    ) AS congestion_fee_percentage
FROM silver.fact_taxi_trips f
JOIN silver.dim_zone z ON f.pickup_location_id = z.location_id
JOIN silver.dim_taxi_type t ON f.taxi_type_id = t.taxi_type_id
WHERE f.cbd_congestion_fee > 0
GROUP BY z.borough, t.taxi_type_name
ORDER BY total_congestion_fees_collected DESC;


-- ==============================================================================
-- 3. Traffic Velocity: Peak vs. Off-Peak Speeds
-- Business Insight: Calculates the effective travel speed in miles per hour (MPH) 
-- to identify severe traffic bottlenecks. It leverages the extracted time dimension 
-- and derived duration metrics.
-- ==============================================================================

SELECT 
    dt.hour,
    dt.is_weekend,
    COUNT(f.trip_id) AS trip_volume,
    ROUND(AVG(f.trip_distance), 2) AS avg_distance_miles,
    ROUND(AVG(f.trip_duration_seconds / 60.0), 2) AS avg_duration_minutes,
    ROUND(
        AVG(f.trip_distance / NULLIF(f.trip_duration_seconds / 3600.0, 0)), 
        2
    ) AS avg_speed_mph
FROM silver.fact_taxi_trips f
JOIN silver.dim_time dt ON f.pickup_time_id = dt.time_id
WHERE f.trip_distance > 0 
  AND f.trip_duration_seconds > 60 -- Filter out erroneous sub-minute trips
GROUP BY dt.hour, dt.is_weekend
ORDER BY dt.is_weekend, dt.hour;


-- ==============================================================================
-- 4. Market Share: The Airport Corridors
-- Business Insight: Analyzes the dominance of Yellow versus Green taxis for highly 
-- lucrative airport routes, calculating the average fare and trip distances.
-- ==============================================================================

SELECT 
    z.zone_name AS airport_destination,
    t.taxi_type_name,
    COUNT(f.trip_id) AS total_dropoffs,
    ROUND(AVG(f.total_amount), 2) AS average_fare,
    ROUND(AVG(f.trip_distance), 2) AS average_distance_miles
FROM silver.fact_taxi_trips f
JOIN silver.dim_zone z ON f.dropoff_location_id = z.location_id
JOIN silver.dim_taxi_type t ON f.taxi_type_id = t.taxi_type_id
WHERE z.zone_name ILIKE '%Airport%'
GROUP BY z.zone_name, t.taxi_type_name
ORDER BY total_dropoffs DESC;


-- ==============================================================================
-- 5. Consumer Behavior: Tipping Elasticity
-- Business Insight: Uses conditional logic to bucket trip distances into categories, 
-- evaluating if passengers tip a higher percentage on short hops or long hauls.
-- ==============================================================================

WITH trip_buckets AS (
    SELECT 
        trip_id,
        tip_amount,
        fare_amount,
        CASE 
            WHEN trip_distance <= 2.0 THEN '1. Micro (0-2 miles)'
            WHEN trip_distance <= 5.0 THEN '2. Short (2-5 miles)'
            WHEN trip_distance <= 10.0 THEN '3. Medium (5-10 miles)'
            ELSE '4. Long (10+ miles)'
        END AS distance_bracket
    FROM silver.fact_taxi_trips f
    JOIN silver.dim_payment_type p ON f.payment_type_id = p.payment_type_id
    WHERE p.payment_description = 'Credit card' -- Cash tips are not recorded
      AND f.fare_amount > 0
)
SELECT 
    distance_bracket,
    COUNT(*) AS total_credit_card_trips,
    ROUND(AVG(tip_amount), 2) AS average_tip_usd,
    ROUND(
        AVG(tip_amount / fare_amount) * 100, 
        2
    ) AS average_tip_percentage
FROM trip_buckets
GROUP BY distance_bracket
ORDER BY distance_bracket;
