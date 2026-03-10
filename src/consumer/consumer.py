import json
import os
import time
import threading
import signal
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer

import requests
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from dotenv import load_dotenv
import psycopg2
from psycopg2 import OperationalError

# Load environment variables
load_dotenv()

# Configuration
API_KEY = os.getenv('OPENWEATHER_API_KEY')
# If API_KEY isn't set via env, try to read from Docker secret mount
if not API_KEY:
    secret_path = '/run/secrets/openweather_api_key'
    try:
        if os.path.exists(secret_path):
            with open(secret_path, 'r') as sf:
                API_KEY = sf.read().strip()
                print('Loaded OpenWeather API key from secret file')
    except Exception:
        API_KEY = None

KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'logistics-stream')
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'localhost')
POSTGRES_DB = os.getenv('POSTGRES_DB', 'logistics_reports')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'admin')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'password123')
HEALTH_PORT = int(os.getenv('CONSUMER_HEALTH_PORT', '8080'))

# Runtime state for health checks
state = {
    'running': True,
    'kafka_connected': False,
    'db_connected': False,
    'table_exists': False,
    'last_processed_id': None,
}

# Create Kafka consumer with retry (broker may not be ready yet)
consumer = None
max_kafka_attempts = 30
k_attempt = 0
while k_attempt < max_kafka_attempts and consumer is None and state['running']:
    try:
        k_attempt += 1
        print(f"Connecting to Kafka broker ({KAFKA_BROKER}) - attempt {k_attempt}/{max_kafka_attempts}...")
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=[KAFKA_BROKER],
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='logistics-consumer-group'
        )
        state['kafka_connected'] = True
    except NoBrokersAvailable:
        print("Kafka broker not available yet - sleeping 2s and retrying...")
        time.sleep(2)

if consumer is None:
    raise RuntimeError(f"Could not connect to Kafka broker at {KAFKA_BROKER} after {max_kafka_attempts} attempts")

# Database connection with retry
conn = None
max_db_attempts = 30
db_attempt = 0
while db_attempt < max_db_attempts and conn is None and state['running']:
    try:
        db_attempt += 1
        print(f"Connecting to Postgres ({POSTGRES_HOST}) - attempt {db_attempt}/{max_db_attempts}...")
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )
        # Use autocommit so single INSERTs don't leave a long-lived transaction
        conn.autocommit = True
        state['db_connected'] = True
    except OperationalError as e:
        print(f"Postgres not ready ({e}) - sleeping 2s and retrying...")
        time.sleep(2)

if conn is None:
    raise RuntimeError(f"Could not connect to Postgres at {POSTGRES_HOST} after {max_db_attempts} attempts")


def ensure_table_exists():
    """Create the weather_alerts table if it doesn't exist and update state['table_exists']."""
    create_sql = """
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
    """
    try:
        with conn.cursor() as cur:
            cur.execute(create_sql)
            # create a simple index to speed time-range queries
            cur.execute("CREATE INDEX IF NOT EXISTS idx_event_timestamp ON weather_alerts(event_timestamp);")
        state['table_exists'] = True
        print('Ensured weather_alerts table exists')
    except Exception as e:
        state['table_exists'] = False
        print(f'Failed to ensure table exists: {e}')


def save_to_db(data, risk, weather_info):
    query = """
    INSERT INTO weather_alerts (truck_id, lat, lon, weather_main, wind_speed, risk_level, event_timestamp)
    VALUES (%s, %s, %s, %s, %s, %s, TO_TIMESTAMP(%s))
    """
    try:
        # Use a fresh cursor for each operation to avoid cursor-level aborted states
        with conn.cursor() as cur_local:
            cur_local.execute(query, (
                data.get('truck_id'),
                data.get('lat'),
                data.get('lon'),
                weather_info.get('main'),
                weather_info.get('wind'),
                risk,
                data.get('timestamp')
            ))
            # If autocommit is off, commit explicitly
            if not conn.autocommit:
                conn.commit()
    except Exception as e:
        # Roll back the current transaction so the connection isn't left in an
        # aborted state. Re-raise so the caller can log/handle the failure.
        try:
            conn.rollback()
        except Exception:
            pass
        raise


def get_weather_risk(lat, lon):
    # If coordinates are missing, avoid calling the weather API and return UNKNOWN
    if lat is None or lon is None:
        return "UNKNOWN", {"main": "Unknown", "wind": 0}

    # If API key is not provided, skip external call (better than embedding secrets)
    if not API_KEY:
        return "UNKNOWN", {"main": "Unknown", "wind": 0}

    url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={API_KEY}"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        payload = response.json()
    except Exception as e:
        print(f"Failed to fetch weather: {e}")
        # Return unknown/low risk by default when API fails
        return "UNKNOWN", {"main": "Unknown", "wind": 0}

    # Extract condition and wind
    condition = payload.get('weather', [{}])[0].get('main', 'Clear')
    wind_speed = payload.get('wind', {}).get('speed', 0)

    weather_info = {"main": condition, "wind": wind_speed}

    # Logic: If wind speed > 15m/s or heavy precipitation, it's "HIGH_RISK"
    if condition in ['Rain', 'Snow', 'Thunderstorm'] or wind_speed > 15:
        return "HIGH_RISK", weather_info
    return "LOW_RISK", weather_info


# Health HTTP server
class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path not in ['/health', '/ready']:
            self.send_response(404)
            self.end_headers()
            return
        payload = {
            'running': state['running'],
            'kafka_connected': state['kafka_connected'],
            'db_connected': state['db_connected'],
            'table_exists': state.get('table_exists'),
            'last_processed_id': state.get('last_processed_id')
        }
        body = json.dumps(payload).encode('utf-8')
        # If readiness check and table/db not ready, return 503
        if self.path == '/ready' and (not state['db_connected'] or not state['table_exists'] or not state['kafka_connected']):
            self.send_response(503)
        else:
            self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def start_health_server():
    server = HTTPServer(('0.0.0.0', HEALTH_PORT), HealthHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    print(f"Health server listening on 0.0.0.0:{HEALTH_PORT}")
    return server


# Ensure the table is present before starting the consumer loop
ensure_table_exists()

# Consumer loop using poll so shutdown is responsive
print("Listening for truck updates...")

health_server = start_health_server()


def _shutdown(signum, frame):
    print(f"Received signal {signum}, shutting down...")
    state['running'] = False
    try:
        if consumer is not None:
            try:
                consumer.close()
            except Exception:
                pass
    except Exception:
        pass
    try:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
    except Exception:
        pass
    try:
        # shutdown health server gracefully
        health_server.shutdown()
    except Exception:
        pass
    sys.exit(0)

# Register signal handlers for graceful termination
signal.signal(signal.SIGINT, _shutdown)
signal.signal(signal.SIGTERM, _shutdown)

try:
    while state['running']:
        # poll for messages in 1 second windows
        records = consumer.poll(timeout_ms=1000)
        for tp, recs in records.items():
            for rec in recs:
                try:
                    truck_data = rec.value
                    risk_level, weather = get_weather_risk(truck_data.get('lat'), truck_data.get('lon'))

                    print(f"--- Alert ---")
                    print(f"Truck: {truck_data.get('truck_id')} | Status: {truck_data.get('status')}")
                    print(f"Weather: {weather.get('main')} | Wind: {weather.get('wind')} | RISK LEVEL: {risk_level}")

                    # Persist to DB (best effort)
                    try:
                        # Attempt to save; if table was missing, try to create it and retry once
                        try:
                            save_to_db(truck_data, risk_level, weather)
                        except Exception as e:
                            print(f"Failed to save to DB on first attempt: {e}")
                            # Try ensuring the table exists and retry once
                            ensure_table_exists()
                            try:
                                save_to_db(truck_data, risk_level, weather)
                            except Exception as e2:
                                print(f"Failed to save to DB after ensuring table: {e2}")
                                raise

                        # update state for health checks
                        state['last_processed_id'] = getattr(rec, 'offset', None)
                    except Exception as e:
                        print(f"Failed to save to DB: {e}")

                except Exception as e:
                    print(f"Error processing message: {e}")
        # small sleep so loop is friendly if no records
        time.sleep(0.1)
except KeyboardInterrupt:
    _shutdown(None, None)
except Exception as e:
    print(f"Unhandled exception in consumer loop: {e}")
    _shutdown(None, None)
