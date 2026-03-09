import json
import os

import requests
from kafka import KafkaConsumer
from dotenv import load_dotenv
# Configuration
API_KEY = os.getenv('OPENWEATHER_API_KEY')
KAFKA_TOPIC = "logistics-stream"

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

import psycopg2

# Database connection details
conn = psycopg2.connect(
    host="localhost",
    database="logistics_reports",
    user="admin",
    password="password123"
)
cur = conn.cursor()

def save_to_db(data, risk, weather_info):
    query = """
    INSERT INTO weather_alerts (truck_id, lat, lon, weather_main, wind_speed, risk_level, event_timestamp)
    VALUES (%s, %s, %s, %s, %s, %s, TO_TIMESTAMP(%s))
    """
    cur.execute(query, (
        data['truck_id'],
        data['lat'],
        data['lon'],
        weather_info['main'],
        weather_info['wind'],
        risk,
        data['timestamp']
    ))
    conn.commit()

def get_weather_risk(lat, lon):
    url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={API_KEY}"
    response = requests.get(url).json()

    # Logic: If wind speed > 15m/s or heavy rain, it's "High Risk"
    condition = response.get('weather', [{}])[0].get('main', 'Clear')
    wind_speed = response.get('wind', {}).get('speed', 0)

    if condition in ['Rain', 'Snow', 'Thunderstorm'] or wind_speed > 15:
        return "HIGH_RISK", condition
    return "LOW_RISK", condition


print("Listening for truck updates...")
for message in consumer:
    truck_data = message.value
    risk_level, weather = get_weather_risk(truck_data['lat'], truck_data['lon'])

    print(f"--- Alert ---")
    print(f"Truck: {truck_data['truck_id']} | Status: {truck_data['status']}")
    print(f"Weather: {weather} | RISK LEVEL: {risk_level}")