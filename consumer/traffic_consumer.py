from kafka import KafkaConsumer
import json
import pandas as pd
import folium
from folium.plugins import HeatMap
import os
import time
import snowflake.connector
from datetime import datetime
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class HamburgTrafficConsumer:
    def __init__(self):
        # Paths
        self.BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        self.DATA_DIR = os.path.join(self.BASE_DIR, 'data')
        self.CONFIG_PATH = os.path.join(self.BASE_DIR, 'snowflake_config.json')
        os.makedirs(self.DATA_DIR, exist_ok=True)

        self.CSV_PATH = os.path.join(self.DATA_DIR, 'traffic_data.csv')
        self.MAP_PATH = os.path.join(self.DATA_DIR, 'traffic_map.html')

        # Load Snowflake config
        with open(self.CONFIG_PATH) as f:
            self.snowflake_cfg = json.load(f)
        
        # Initialize connections
        self.setup_snowflake_connection()
        self.setup_kafka_consumer()
        
        # Load existing data
        self.load_existing_data()
    
    def setup_snowflake_connection(self):
        """Setup Snowflake connection with error handling"""
        try:
            self.conn = snowflake.connector.connect(
                user=self.snowflake_cfg["user"],
                password=self.snowflake_cfg["password"],
                account=self.snowflake_cfg["account"],
                warehouse=self.snowflake_cfg["warehouse"],
                database=self.snowflake_cfg["database"],
                schema=self.snowflake_cfg["schema"]
            )
            self.cursor = self.conn.cursor()
            logger.info("✅ Snowflake connection established")
            
            # Test the connection with our new table
            self.cursor.execute("SELECT COUNT(*) FROM HAMBURG_TRAFFIC_LIVE")
            count = self.cursor.fetchone()[0]
            logger.info(f"📊 Current records in HAMBURG_TRAFFIC_LIVE: {count}")
            
        except Exception as e:
            logger.error(f"❌ Snowflake connection failed: {e}")
            raise
    
    def setup_kafka_consumer(self):
        """Setup Kafka consumer"""
        try:
            self.consumer = KafkaConsumer(
                'hamburg_traffic',
                bootstrap_servers='localhost:9092',
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id='traffic-group'
            )
            logger.info("✅ Kafka consumer initialized")
        except Exception as e:
            logger.error(f"❌ Kafka consumer setup failed: {e}")
            raise
    
    def load_existing_data(self):
        """Load existing CSV data"""
        if os.path.exists(self.CSV_PATH):
            try:
                self.df = pd.read_csv(self.CSV_PATH)
                logger.info(f"📂 Loaded {len(self.df)} existing records from CSV")
            except Exception as e:
                logger.warning(f"⚠️ Error loading CSV: {e}. Starting fresh.")
                self.df = self.create_empty_dataframe()
        else:
            self.df = self.create_empty_dataframe()
            logger.info("📊 Starting with empty dataset")
    
    def create_empty_dataframe(self):
        """Create empty DataFrame with correct schema"""
        return pd.DataFrame(columns=[
            'timestamp', 'latitude', 'longitude', 'speed', 'vehicle_count', 
            'zone', 'is_major_road', 'traffic_density'
        ])
    
    def generate_map(self, dataframe, max_points=200):
        """Generate Folium heatmap from traffic data"""
        try:
            if dataframe.empty:
                logger.warning("⚠️ No data available for map generation")
                return
            
            # Use latest data points for map
            recent_data = dataframe.tail(max_points)
            
            # Center map on Hamburg
            m = folium.Map(location=[53.5511, 9.9937], zoom_start=11)
            
            # Prepare heat data: [lat, lon, weight]
            heat_data = []
            for _, row in recent_data.iterrows():
                if pd.notna(row['latitude']) and pd.notna(row['longitude']):
                    # Use vehicle_count as weight for heatmap intensity
                    weight = max(row['vehicle_count'], 1)
                    heat_data.append([row['latitude'], row['longitude'], weight])
            
            if heat_data:
                # Add heatmap layer
                HeatMap(
                    heat_data, 
                    radius=20, 
                    blur=15, 
                    gradient={0.4: 'blue', 0.65: 'lime', 0.85: 'orange', 1.0: 'red'}
                ).add_to(m)
                
                # Add some individual markers for major roads
                major_roads = recent_data[recent_data['is_major_road'] == True].tail(10)
                for _, row in major_roads.iterrows():
                    folium.CircleMarker(
                        location=[row['latitude'], row['longitude']],
                        radius=8,
                        popup=f"🛣️ {row['zone']}<br>Speed: {row['speed']} km/h<br>Vehicles: {row['vehicle_count']}",
                        color='purple',
                        fill=True,
                        fillColor='purple',
                        fillOpacity=0.7
                    ).add_to(m)
            
            m.save(self.MAP_PATH)
            logger.info(f"🗺️ Map updated with {len(heat_data)} data points")
            
        except Exception as e:
            logger.error(f"❌ Map generation failed: {e}")
    
    def insert_to_snowflake(self, record):
        """Insert traffic record to Snowflake using new table schema"""
        try:
            # Convert timestamp to proper format
            if 'T' in record['timestamp']:
                timestamp = datetime.fromisoformat(record['timestamp'].replace('Z', '+00:00'))
            else:
                timestamp = datetime.fromisoformat(record['timestamp'])
            
            # Use the new table name and column names
            sql = """
                INSERT INTO HAMBURG_TRAFFIC_LIVE (
                    record_timestamp, lat, lon, speed_kmh, vehicle_count, 
                    zone_name, is_major_road, traffic_density
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            values = (
                timestamp,
                record['location']['lat'],
                record['location']['lon'],
                record['speed'],
                record['vehicle_count'],
                record.get('zone', 'Unknown'),
                record.get('is_major_road', False),
                record.get('traffic_density', 0.5)
            )
            
            self.cursor.execute(sql, values)
            self.conn.commit()
            logger.debug("✅ Record inserted to Snowflake HAMBURG_TRAFFIC_LIVE")
            
        except Exception as e:
            logger.error(f"❌ Snowflake insert failed: {e}")
            # Don't stop processing on Snowflake errors
    
    def process_message(self, record):
        """Process a single Kafka message"""
        try:
            zone = record.get('zone', 'Unknown')
            vehicles = record['vehicle_count']
            speed = record['speed']
            logger.info(f"📥 Processing: {zone} - {vehicles} vehicles @ {speed}km/h")
            
            # Create new row with consistent column names for CSV
            new_row = {
                'timestamp': record['timestamp'],
                'latitude': record['location']['lat'],
                'longitude': record['location']['lon'],
                'speed': record['speed'],
                'vehicle_count': record['vehicle_count'],
                'zone': record.get('zone', 'Unknown'),
                'is_major_road': record.get('is_major_road', False),
                'traffic_density': record.get('traffic_density', 0.5)
            }

            # Add to DataFrame
            self.df = pd.concat([self.df, pd.DataFrame([new_row])], ignore_index=True)
            
            # Keep only last 5000 records in memory to prevent memory issues
            if len(self.df) > 5000:
                self.df = self.df.tail(5000).reset_index(drop=True)
            
            # Save to CSV every 5 records
            if len(self.df) % 5 == 0:
                self.df.to_csv(self.CSV_PATH, index=False)
            
            # Generate map every 20 records (adjust as needed)
            if len(self.df) % 20 == 0:
                self.generate_map(self.df)
            
            # Insert to Snowflake
            self.insert_to_snowflake(record)
            
        except Exception as e:
            logger.error(f"❌ Error processing message: {e}")
    
    def get_snowflake_stats(self):
        """Get current statistics from Snowflake"""
        try:
            self.cursor.execute("""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(DISTINCT zone_name) as unique_zones,
                    AVG(speed_kmh) as avg_speed,
                    AVG(vehicle_count) as avg_vehicles
                FROM HAMBURG_TRAFFIC_LIVE 
                WHERE record_timestamp > DATEADD(HOUR, -1, CURRENT_TIMESTAMP())
            """)
            stats = self.cursor.fetchone()
            if stats and stats[0] > 0:
                logger.info(f"📊 Last hour: {stats[0]} records, {stats[1]} zones, avg speed: {stats[2]:.1f}km/h, avg vehicles: {stats[3]:.1f}")
        except Exception as e:
            logger.error(f"❌ Error getting Snowflake stats: {e}")
    
    def start_consuming(self):
        """Start consuming messages from Kafka"""
        logger.info("🚦 Hamburg Traffic Consumer started...")
        logger.info(f"📊 Current CSV data points: {len(self.df)}")
        
        # Show initial stats
        self.get_snowflake_stats()
        
        record_count = 0
        try:
            for message in self.consumer:
                record = message.value
                self.process_message(record)
                
                record_count += 1
                # Show stats every 50 records
                if record_count % 50 == 0:
                    self.get_snowflake_stats()
                
        except KeyboardInterrupt:
            logger.info("🛑 Consumer stopped by user")
        except Exception as e:
            logger.error(f"❌ Consumer error: {e}")
        finally:
            self.cleanup()
    
    def cleanup(self):
        """Cleanup connections"""
        try:
            # Final save
            if hasattr(self, 'df') and not self.df.empty:
                self.df.to_csv(self.CSV_PATH, index=False)
                logger.info(f"💾 Final CSV saved with {len(self.df)} records")
            
            if hasattr(self, 'cursor'):
                self.cursor.close()
            if hasattr(self, 'conn'):
                self.conn.close()
            if hasattr(self, 'consumer'):
                self.consumer.close()
            logger.info("🧹 Cleanup completed")
        except Exception as e:
            logger.error(f"❌ Cleanup error: {e}")

if __name__ == "__main__":
    consumer = HamburgTrafficConsumer()
    consumer.start_consuming()