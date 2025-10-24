-- EV Charging Station Analytics Database Schema (PostgreSQL)

-- Charging Stations Table
CREATE TABLE IF NOT EXISTS charging_stations (
    id VARCHAR(255) PRIMARY KEY,
    name VARCHAR(500),
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8),
    address TEXT,
    city VARCHAR(255),
    state VARCHAR(255),
    country VARCHAR(255),
    operator VARCHAR(255),
    network VARCHAR(255),
    status VARCHAR(50),
    access_type VARCHAR(100),
    pricing_info TEXT,
    amenities TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Charging Points Table
CREATE TABLE IF NOT EXISTS charging_points (
    id VARCHAR(255) PRIMARY KEY,
    station_id VARCHAR(255),
    connector_type VARCHAR(100),
    power_kw DECIMAL(8, 2),
    voltage DECIMAL(8, 2),
    amperage DECIMAL(8, 2),
    status VARCHAR(50),
    last_updated TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (station_id) REFERENCES charging_stations(id) ON DELETE CASCADE
);

-- Usage Data Table
CREATE TABLE IF NOT EXISTS usage_data (
    id SERIAL PRIMARY KEY,
    station_id VARCHAR(255),
    point_id VARCHAR(255),
    session_start TIMESTAMP,
    session_end TIMESTAMP,
    energy_consumed_kwh DECIMAL(10, 3),
    duration_minutes INTEGER,
    cost DECIMAL(10, 2),
    user_type VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (station_id) REFERENCES charging_stations(id) ON DELETE CASCADE,
    FOREIGN KEY (point_id) REFERENCES charging_points(id) ON DELETE CASCADE
);

-- Weather Data Table
CREATE TABLE IF NOT EXISTS weather_data (
    id SERIAL PRIMARY KEY,
    station_id VARCHAR(255),
    timestamp TIMESTAMP,
    temperature_celsius DECIMAL(5, 2),
    humidity_percent DECIMAL(5, 2),
    pressure_hpa DECIMAL(8, 2),
    wind_speed_ms DECIMAL(5, 2),
    wind_direction_degrees INTEGER,
    precipitation_mm DECIMAL(8, 2),
    weather_condition VARCHAR(100),
    visibility_km DECIMAL(5, 2),
    uv_index DECIMAL(3, 1),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (station_id) REFERENCES charging_stations(id) ON DELETE CASCADE
);

-- Create index for weather data
CREATE INDEX IF NOT EXISTS idx_weather_station_time ON weather_data(station_id, timestamp);

-- Traffic Data Table
CREATE TABLE IF NOT EXISTS traffic_data (
    id SERIAL PRIMARY KEY,
    station_id VARCHAR(255),
    timestamp TIMESTAMP,
    traffic_density DECIMAL(5, 2),
    average_speed_kmh DECIMAL(5, 2),
    congestion_level VARCHAR(50),
    road_type VARCHAR(100),
    distance_to_station_km DECIMAL(8, 3),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (station_id) REFERENCES charging_stations(id) ON DELETE CASCADE
);

-- Create index for traffic data
CREATE INDEX IF NOT EXISTS idx_traffic_station_time ON traffic_data(station_id, timestamp);

-- Station Status History Table
CREATE TABLE IF NOT EXISTS station_status_history (
    id SERIAL PRIMARY KEY,
    station_id VARCHAR(255),
    timestamp TIMESTAMP,
    status VARCHAR(50),
    available_points INTEGER,
    total_points INTEGER,
    average_wait_time_minutes INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (station_id) REFERENCES charging_stations(id) ON DELETE CASCADE
);

-- Create index for status history
CREATE INDEX IF NOT EXISTS idx_status_station_time ON station_status_history(station_id, timestamp);

-- Anomaly Detection Results Table
CREATE TABLE IF NOT EXISTS anomaly_detection (
    id SERIAL PRIMARY KEY,
    station_id VARCHAR(255),
    anomaly_type VARCHAR(100),
    severity_score DECIMAL(5, 3),
    detected_at TIMESTAMP,
    description TEXT,
    is_resolved BOOLEAN DEFAULT FALSE,
    resolved_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (station_id) REFERENCES charging_stations(id) ON DELETE CASCADE
);

-- Create index for anomaly detection
CREATE INDEX IF NOT EXISTS idx_anomaly_station_time ON anomaly_detection(station_id, detected_at);

-- Feature Engineering Table
CREATE TABLE IF NOT EXISTS engineered_features (
    id SERIAL PRIMARY KEY,
    station_id VARCHAR(255),
    date DATE,
    hour_of_day INTEGER,
    day_of_week INTEGER,
    is_weekend BOOLEAN,
    is_holiday BOOLEAN,
    avg_downtime_minutes DECIMAL(8, 2),
    energy_consumption_per_traffic DECIMAL(10, 3),
    usage_spike_during_storm BOOLEAN,
    peak_usage_hours INTEGER,
    avg_wait_time_minutes DECIMAL(8, 2),
    total_sessions INTEGER,
    total_energy_kwh DECIMAL(10, 3),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (station_id) REFERENCES charging_stations(id) ON DELETE CASCADE
);

-- Create index for engineered features
CREATE INDEX IF NOT EXISTS idx_features_station_date ON engineered_features(station_id, date);

-- Energy Consumption Table
CREATE TABLE IF NOT EXISTS energy_consumption (
    id SERIAL PRIMARY KEY,
    station_id VARCHAR(255),
    date DATE,
    total_energy_kwh DECIMAL(10, 3),
    peak_hour_energy DECIMAL(10, 3),
    off_peak_energy DECIMAL(10, 3),
    session_count INTEGER,
    avg_session_duration INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (station_id) REFERENCES charging_stations(id) ON DELETE CASCADE
);

-- Create index for energy consumption
CREATE INDEX IF NOT EXISTS idx_energy_station_date ON energy_consumption(station_id, date);

-- Data Collection Log Table
CREATE TABLE IF NOT EXISTS data_collection_log (
    id SERIAL PRIMARY KEY,
    data_source VARCHAR(100),
    collection_type VARCHAR(100),
    records_collected INTEGER,
    status VARCHAR(50),
    error_message TEXT,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
