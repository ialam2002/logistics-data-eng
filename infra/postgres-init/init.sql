CREATE TABLE IF NOT EXISTS weather_alerts (
    id SERIAL PRIMARY KEY,
    truck_id VARCHAR(50) NOT NULL,
    lat FLOAT,
    lon FLOAT,
    weather_main VARCHAR(50),
    wind_speed FLOAT,
    risk_level VARCHAR(20),
    event_timestamp TIMESTAMP,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);