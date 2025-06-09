from kafka import KafkaConsumer
import json
import pandas as pd
import folium
from folium.plugins import HeatMap
import os
import time
import snowflake.connector

# Paths
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
DATA_DIR = os.path.join(BASE_DIR, 'data')
CONFIG_PATH = os.path.join(BASE_DIR, 'snowflake_config.json')
os.makedirs(DATA_DIR, exist_ok=True)

CSV_PATH = os.path.join(DATA_DIR, 'traffic_data.csv')
MAP_PATH = os.path.join(DATA_DIR, 'traffic_map.html')

# Load Snowflake credentials from config
import json
with open(CONFIG_PATH) as f:
    snowflake_cfg = json.load(f)

# Connect to Snowflake
conn = snowflake.connector.connect(
    user=snowflake_cfg["user"],
    password=snowflake_cfg["password"],
    account=snowflake_cfg["account"],
    warehouse=snowflake_cfg["warehouse"],
    database=snowflake_cfg["database"],
    schema=snowflake_cfg["schema"]
)
cursor = conn.cursor()

# Kafka consumer
consumer = KafkaConsumer(
    'hamburg_traffic',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='traffic-group'
)

# Load existing data
if os.path.exists(CSV_PATH):
    df = pd.read_csv(CSV_PATH)
else:
    df = pd.DataFrame(columns=['timestamp', 'lat', 'lon', 'speed', 'vehicle_count'])

# Map generation
def generate_map(dataframe):
    m = folium.Map(location=[53.55, 10.0], zoom_start=12)
    heat_data = [[row['lat'], row['lon'], row['vehicle_count']] for _, row in dataframe.iterrows()]
    HeatMap(heat_data, radius=15).add_to(m)
    m.save(MAP_PATH)
    print("Map updated with", len(dataframe), "rows")

# Snowflake insert function
def insert_to_snowflake(row):
    try:
        cursor.execute("""
            INSERT INTO LIVE_STREAM (timestamp, lat, lon, speed, vehicle_count)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            row['timestamp'],
            row['lat'],
            row['lon'],
            row['speed'],
            row['vehicle_count']
        ))
        conn.commit()
    except Exception as e:
        print("❌ Snowflake insert failed:", e)

print("✅ Kafka consumer started...")

try:
    for message in consumer:
        record = message.value
        print("📥 Received:", record)

        new_row = {
            'timestamp': record['timestamp'],
            'lat': record['location']['lat'],
            'lon': record['location']['lon'],
            'speed': record['speed'],
            'vehicle_count': record['vehicle_count']
        }

        # Save locally
        df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
        df.to_csv(CSV_PATH, index=False)
        generate_map(df.tail(100))

        # Insert to Snowflake
        insert_to_snowflake(new_row)

except KeyboardInterrupt:
    print("🛑 Consumer stopped.")

finally:
    cursor.close()
    conn.close()
