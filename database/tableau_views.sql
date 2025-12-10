-- SQL Views for Tableau Visualization
-- These views aggregate and prepare data for easy visualization in Tableau

-- 1. Station Overview View
-- Provides comprehensive station information with aggregated metrics
CREATE OR REPLACE VIEW tableau_station_overview AS
SELECT 
    cs.id AS station_id,
    cs.name AS station_name,
    cs.latitude,
    cs.longitude,
    cs.city,
    cs.state,
    cs.country,
    cs.operator,
    cs.network,
    cs.status,
    cs.access_type,
    COUNT(DISTINCT cp.id) AS total_charging_points,
    AVG(cp.power_kw) AS avg_power_kw,
    MAX(cp.power_kw) AS max_power_kw,
    COUNT(DISTINCT ud.id) AS total_sessions,
    SUM(ud.energy_consumed_kwh) AS total_energy_kwh,
    AVG(ud.duration_minutes) AS avg_session_duration_minutes,
    AVG(ud.cost) AS avg_session_cost,
    COUNT(DISTINCT wd.id) AS weather_records_count,
    COUNT(DISTINCT td.id) AS traffic_records_count,
    COUNT(DISTINCT ad.id) AS anomaly_count,
    MAX(ud.session_start) AS last_session_date,
    cs.created_at,
    cs.updated_at
FROM charging_stations cs
LEFT JOIN charging_points cp ON cs.id = cp.station_id
LEFT JOIN usage_data ud ON cs.id = ud.station_id
LEFT JOIN weather_data wd ON cs.id = wd.station_id
LEFT JOIN traffic_data td ON cs.id = td.station_id
LEFT JOIN anomaly_detection ad ON cs.id = ad.station_id AND ad.is_resolved = FALSE
GROUP BY cs.id, cs.name, cs.latitude, cs.longitude, cs.city, cs.state, 
         cs.country, cs.operator, cs.network, cs.status, cs.access_type,
         cs.created_at, cs.updated_at;

-- 2. Usage Patterns View
-- Time-based usage patterns for trend analysis
CREATE OR REPLACE VIEW tableau_usage_patterns AS
SELECT 
    ud.station_id,
    cs.name AS station_name,
    cs.city,
    cs.state,
    DATE(ud.session_start) AS session_date,
    EXTRACT(HOUR FROM ud.session_start) AS hour_of_day,
    EXTRACT(DOW FROM ud.session_start) AS day_of_week,
    CASE WHEN EXTRACT(DOW FROM ud.session_start) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend,
    COUNT(*) AS session_count,
    SUM(ud.energy_consumed_kwh) AS total_energy_kwh,
    AVG(ud.energy_consumed_kwh) AS avg_energy_kwh,
    AVG(ud.duration_minutes) AS avg_duration_minutes,
    SUM(ud.cost) AS total_cost,
    AVG(ud.cost) AS avg_cost
FROM usage_data ud
JOIN charging_stations cs ON ud.station_id = cs.id
GROUP BY ud.station_id, cs.name, cs.city, cs.state, 
         DATE(ud.session_start), EXTRACT(HOUR FROM ud.session_start),
         EXTRACT(DOW FROM ud.session_start);

-- 3. Weather Correlation View
-- Weather data with usage metrics for correlation analysis
CREATE OR REPLACE VIEW tableau_weather_correlation AS
SELECT 
    wd.station_id,
    cs.name AS station_name,
    wd.timestamp,
    DATE(wd.timestamp) AS weather_date,
    EXTRACT(HOUR FROM wd.timestamp) AS hour_of_day,
    wd.temperature_celsius,
    wd.humidity_percent,
    wd.pressure_hpa,
    wd.wind_speed_ms,
    wd.precipitation_mm,
    wd.weather_condition,
    wd.uv_index,
    COUNT(ud.id) AS sessions_during_weather,
    SUM(ud.energy_consumed_kwh) AS energy_during_weather,
    AVG(ud.duration_minutes) AS avg_duration_during_weather
FROM weather_data wd
JOIN charging_stations cs ON wd.station_id = cs.id
LEFT JOIN usage_data ud ON wd.station_id = ud.station_id 
    AND DATE_TRUNC('hour', ud.session_start) = DATE_TRUNC('hour', wd.timestamp)
GROUP BY wd.station_id, cs.name, wd.timestamp, wd.temperature_celsius,
         wd.humidity_percent, wd.pressure_hpa, wd.wind_speed_ms,
         wd.precipitation_mm, wd.weather_condition, wd.uv_index;

-- 4. Traffic Correlation View
-- Traffic data with usage metrics for correlation analysis
CREATE OR REPLACE VIEW tableau_traffic_correlation AS
SELECT 
    td.station_id,
    cs.name AS station_name,
    td.timestamp,
    DATE(td.timestamp) AS traffic_date,
    EXTRACT(HOUR FROM td.timestamp) AS hour_of_day,
    td.traffic_density,
    td.average_speed_kmh,
    td.congestion_level,
    td.road_type,
    td.distance_to_station_km,
    COUNT(ud.id) AS sessions_during_traffic,
    SUM(ud.energy_consumed_kwh) AS energy_during_traffic,
    AVG(ud.duration_minutes) AS avg_duration_during_traffic
FROM traffic_data td
JOIN charging_stations cs ON td.station_id = cs.id
LEFT JOIN usage_data ud ON td.station_id = ud.station_id 
    AND DATE_TRUNC('hour', ud.session_start) = DATE_TRUNC('hour', td.timestamp)
GROUP BY td.station_id, cs.name, td.timestamp, td.traffic_density,
         td.average_speed_kmh, td.congestion_level, td.road_type,
         td.distance_to_station_km;

-- 5. Energy Consumption Trends View
-- Daily energy consumption trends
CREATE OR REPLACE VIEW tableau_energy_trends AS
SELECT 
    ec.station_id,
    cs.name AS station_name,
    cs.city,
    cs.state,
    ec.date,
    EXTRACT(DOW FROM ec.date) AS day_of_week,
    CASE WHEN EXTRACT(DOW FROM ec.date) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend,
    ec.total_energy_kwh,
    ec.peak_hour_energy,
    ec.off_peak_energy,
    ec.session_count,
    ec.avg_session_duration,
    ec.total_energy_kwh / NULLIF(ec.session_count, 0) AS energy_per_session
FROM energy_consumption ec
JOIN charging_stations cs ON ec.station_id = cs.id;

-- 6. Anomaly Summary View
-- Summary of detected anomalies
CREATE OR REPLACE VIEW tableau_anomaly_summary AS
SELECT 
    ad.station_id,
    cs.name AS station_name,
    cs.city,
    cs.state,
    ad.anomaly_type,
    ad.severity_score,
    ad.detected_at,
    DATE(ad.detected_at) AS anomaly_date,
    ad.description,
    ad.is_resolved,
    ad.resolved_at,
    EXTRACT(EPOCH FROM (COALESCE(ad.resolved_at, CURRENT_TIMESTAMP) - ad.detected_at)) / 86400 AS days_open
FROM anomaly_detection ad
JOIN charging_stations cs ON ad.station_id = cs.id;

-- 7. Station Performance View
-- Performance metrics for each station
CREATE OR REPLACE VIEW tableau_station_performance AS
SELECT 
    cs.id AS station_id,
    cs.name AS station_name,
    cs.city,
    cs.state,
    cs.operator,
    COUNT(DISTINCT ud.id) AS total_sessions,
    SUM(ud.energy_consumed_kwh) AS total_energy_kwh,
    AVG(ud.duration_minutes) AS avg_duration_minutes,
    AVG(ud.cost) AS avg_cost,
    SUM(ud.cost) AS total_revenue,
    COUNT(DISTINCT DATE(ud.session_start)) AS active_days,
    MAX(ud.session_start) AS last_session,
    MIN(ud.session_start) AS first_session,
    COUNT(DISTINCT cp.id) AS charging_points_count,
    AVG(cp.power_kw) AS avg_power_kw,
    COUNT(DISTINCT CASE WHEN ad.is_resolved = FALSE THEN ad.id END) AS open_anomalies
FROM charging_stations cs
LEFT JOIN usage_data ud ON cs.id = ud.station_id
LEFT JOIN charging_points cp ON cs.id = cp.station_id
LEFT JOIN anomaly_detection ad ON cs.id = ad.station_id
GROUP BY cs.id, cs.name, cs.city, cs.state, cs.operator;

-- 8. Hourly Usage Heatmap View
-- Hourly usage data for heatmap visualization
CREATE OR REPLACE VIEW tableau_hourly_usage_heatmap AS
SELECT 
    ud.station_id,
    cs.name AS station_name,
    cs.city,
    cs.state,
    EXTRACT(HOUR FROM ud.session_start) AS hour_of_day,
    EXTRACT(DOW FROM ud.session_start) AS day_of_week,
    CASE 
        WHEN EXTRACT(DOW FROM ud.session_start) = 0 THEN 'Sunday'
        WHEN EXTRACT(DOW FROM ud.session_start) = 1 THEN 'Monday'
        WHEN EXTRACT(DOW FROM ud.session_start) = 2 THEN 'Tuesday'
        WHEN EXTRACT(DOW FROM ud.session_start) = 3 THEN 'Wednesday'
        WHEN EXTRACT(DOW FROM ud.session_start) = 4 THEN 'Thursday'
        WHEN EXTRACT(DOW FROM ud.session_start) = 5 THEN 'Friday'
        WHEN EXTRACT(DOW FROM ud.session_start) = 6 THEN 'Saturday'
    END AS day_name,
    COUNT(*) AS session_count,
    SUM(ud.energy_consumed_kwh) AS total_energy_kwh,
    AVG(ud.duration_minutes) AS avg_duration_minutes
FROM usage_data ud
JOIN charging_stations cs ON ud.station_id = cs.id
GROUP BY ud.station_id, cs.name, cs.city, cs.state,
         EXTRACT(HOUR FROM ud.session_start), EXTRACT(DOW FROM ud.session_start);

-- 9. Geographic Distribution View
-- Geographic distribution of stations with metrics
CREATE OR REPLACE VIEW tableau_geographic_distribution AS
SELECT 
    cs.id AS station_id,
    cs.name AS station_name,
    cs.latitude,
    cs.longitude,
    cs.city,
    cs.state,
    cs.country,
    cs.operator,
    cs.status,
    COUNT(DISTINCT ud.id) AS total_sessions,
    SUM(ud.energy_consumed_kwh) AS total_energy_kwh,
    AVG(ud.cost) AS avg_cost,
    COUNT(DISTINCT cp.id) AS charging_points_count
FROM charging_stations cs
LEFT JOIN usage_data ud ON cs.id = ud.station_id
LEFT JOIN charging_points cp ON cs.id = cp.station_id
GROUP BY cs.id, cs.name, cs.latitude, cs.longitude, cs.city, 
         cs.state, cs.country, cs.operator, cs.status;

-- 10. Data Collection Status View
-- Status of data collection activities
CREATE OR REPLACE VIEW tableau_data_collection_status AS
SELECT 
    dcl.id,
    dcl.data_source,
    dcl.collection_type,
    dcl.records_collected,
    dcl.status,
    dcl.error_message,
    dcl.started_at,
    dcl.completed_at,
    DATE(dcl.started_at) AS collection_date,
    EXTRACT(EPOCH FROM (dcl.completed_at - dcl.started_at)) AS duration_seconds,
    CASE 
        WHEN dcl.status = 'success' THEN TRUE 
        ELSE FALSE 
    END AS is_successful
FROM data_collection_log dcl;

