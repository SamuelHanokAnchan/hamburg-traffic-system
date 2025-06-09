from flask import Flask, render_template, jsonify, request
import snowflake.connector
import folium
from folium.plugins import HeatMap
import joblib
import pandas as pd
import numpy as np
import json
import os
from datetime import datetime, timedelta
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)

class HamburgTrafficDashboard:
    def __init__(self):
        """Initialize the Hamburg Traffic Dashboard"""
        self.load_config()
        self.setup_snowflake_connection()
        self.load_models()
        self.load_zone_characteristics()
        logger.info("✅ Hamburg Traffic Dashboard initialized")

    def load_config(self):
        """Load Snowflake configuration"""
        config_path = os.path.join(os.path.dirname(__file__), 'snowflake_config.json')
        with open(config_path, "r") as f:
            self.config = json.load(f)

    def setup_snowflake_connection(self):
        """Setup Snowflake connection"""
        try:
            self.conn = snowflake.connector.connect(
                user=self.config["user"],
                password=self.config["password"],
                account=self.config["account"],
                warehouse=self.config["warehouse"],
                database=self.config["database"],
                schema=self.config["schema"]
            )
            self.cursor = self.conn.cursor()
            logger.info("✅ Snowflake connection established")
        except Exception as e:
            logger.error(f"❌ Snowflake connection failed: {e}")
            raise

    def load_models(self):
        """Load trained models and encoders"""
        try:
            self.vehicle_model = joblib.load('vehicle_model.pkl')
            self.speed_model = joblib.load('speed_model.pkl')
            self.zone_encoder = joblib.load('zone_encoder.pkl')
            self.scaler = joblib.load('scaler.pkl')
            logger.info("✅ Models loaded successfully")
        except Exception as e:
            logger.error(f"❌ Error loading models: {e}")
            self.vehicle_model = None
            self.speed_model = None

    def load_zone_characteristics(self):
        """Load Hamburg zone characteristics"""
        try:
            with open('zone_characteristics.json', 'r') as f:
                self.zone_characteristics = json.load(f)
            logger.info("✅ Zone characteristics loaded")
        except Exception as e:
            logger.warning(f"⚠️ Could not load zone characteristics: {e}")
            # Default characteristics
            self.zone_characteristics = {
                'Altstadt': {'base_traffic': 0.9, 'rush_multiplier': 1.8, 'weekend_factor': 0.6},
                'HafenCity': {'base_traffic': 0.7, 'rush_multiplier': 1.5, 'weekend_factor': 0.8},
                'St. Pauli': {'base_traffic': 0.8, 'rush_multiplier': 1.4, 'weekend_factor': 1.2},
                'Altona': {'base_traffic': 0.6, 'rush_multiplier': 1.6, 'weekend_factor': 0.7},
                'Wandsbek': {'base_traffic': 0.5, 'rush_multiplier': 1.7, 'weekend_factor': 0.5},
                'Harburg': {'base_traffic': 0.4, 'rush_multiplier': 1.3, 'weekend_factor': 0.4},
                'Bergedorf': {'base_traffic': 0.3, 'rush_multiplier': 1.2, 'weekend_factor': 0.3},
                'Blankenese': {'base_traffic': 0.3, 'rush_multiplier': 1.1, 'weekend_factor': 0.6},
                'Rotherbaum': {'base_traffic': 0.7, 'rush_multiplier': 1.5, 'weekend_factor': 0.7},
                'Winterhude': {'base_traffic': 0.6, 'rush_multiplier': 1.4, 'weekend_factor': 0.6}
            }

    def get_total_records(self):
        """Get total number of records in database"""
        try:
            self.cursor.execute("SELECT COUNT(*) as total FROM HAMBURG_TRAFFIC_LIVE")
            return self.cursor.fetchone()[0]
        except Exception as e:
            logger.error(f"❌ Error getting total records: {e}")
            return 0

    def fetch_realtime_data(self, limit=500):
        """Fetch recent traffic data from Snowflake (enhanced)"""
        try:
            query = f"""
            SELECT 
                RECORD_TIMESTAMP,
                LAT,
                LON,
                SPEED_KMH,
                VEHICLE_COUNT,
                ZONE_NAME,
                IS_MAJOR_ROAD,
                TRAFFIC_DENSITY
            FROM HAMBURG_TRAFFIC_LIVE 
            WHERE RECORD_TIMESTAMP > DATEADD(MINUTE, -10, CURRENT_TIMESTAMP())
            ORDER BY RECORD_TIMESTAMP DESC
            LIMIT {limit}
            """
            
            self.cursor.execute(query)
            df = self.cursor.fetch_pandas_all()
            
            if not df.empty:
                df.columns = df.columns.str.lower()
                logger.info(f"✅ Fetched {len(df)} real-time records (last 10 minutes)")
            
            return df
            
        except Exception as e:
            logger.error(f"❌ Error fetching real-time data: {e}")
            return pd.DataFrame()

    def create_prediction_features(self, zone, timestamp):
        """Create feature vector for prediction"""
        hour = timestamp.hour
        day_of_week = timestamp.weekday()
        month = timestamp.month
        is_weekend = int(day_of_week >= 5)
        
        zone_chars = self.zone_characteristics.get(zone, {
            'base_traffic': 0.5, 'rush_multiplier': 1.0, 'weekend_factor': 0.5
        })
        
        # Encode zone (handle unknown zones)
        try:
            zone_encoded = self.zone_encoder.transform([zone])[0]
        except:
            zone_encoded = 0
        
        features = [
            hour, day_of_week, month, is_weekend,
            int(7 <= hour <= 9),  # is_morning_rush
            int(17 <= hour <= 19),  # is_evening_rush
            int(12 <= hour <= 14),  # is_lunch_time
            int(hour <= 6 or hour >= 22),  # is_night
            zone_encoded,
            zone_chars['base_traffic'],
            zone_chars['rush_multiplier'],
            zone_chars['weekend_factor'],
            # Interaction features
            (int(7 <= hour <= 9) + int(17 <= hour <= 19)) * zone_chars['rush_multiplier'],
            is_weekend * zone_chars['weekend_factor'],
            1,  # speed_category (default)
            zone_chars['base_traffic'] * 10,  # vehicle_density
            zone_chars['base_traffic'],  # traffic_density
            zone_chars['base_traffic'] * 8,  # prev_hour_vehicles
            35,  # prev_hour_speed (default)
            0,  # traffic_trend
            53.5511,  # lat (Hamburg center)
            9.9937,  # lon (Hamburg center)
            int('A' in zone or 'B' in zone)  # is_major_road
        ]
        
        return features

    def predict_traffic(self, hours_ahead=1):
        """Predict traffic for specified hours ahead"""
        if not self.vehicle_model or not self.speed_model:
            logger.error("❌ Models not loaded")
            return []
        
        try:
            predictions = []
            current_time = datetime.now()
            future_time = current_time + timedelta(hours=hours_ahead)
            
            # Get zone coordinates for map
            zone_coords = {
                'Altstadt': [53.5511, 9.9937],
                'HafenCity': [53.5407, 9.9961],
                'St. Pauli': [53.5497, 9.9603],
                'Altona': [53.5525, 9.9355],
                'Wandsbek': [53.5685, 10.0714],
                'Harburg': [53.4609, 9.9872],
                'Bergedorf': [53.4892, 10.2314],
                'Blankenese': [53.5644, 9.7975],
                'Rotherbaum': [53.5676, 9.9906],
                'Winterhude': [53.5914, 9.9981]
            }
            
            for zone, coords in zone_coords.items():
                # Create feature vector
                features = self.create_prediction_features(zone, future_time)
                features_scaled = self.scaler.transform([features])
                
                # Predict
                predicted_vehicles = max(1, int(self.vehicle_model.predict(features_scaled)[0]))
                predicted_speed = max(5, int(self.speed_model.predict(features_scaled)[0]))
                
                predictions.append({
                    'zone': zone,
                    'lat': coords[0],
                    'lon': coords[1],
                    'predicted_vehicles': predicted_vehicles,
                    'predicted_speed': predicted_speed,
                    'timestamp': future_time.isoformat()
                })
            
            logger.info(f"✅ Generated {len(predictions)} predictions for {hours_ahead}h ahead")
            return predictions
            
        except Exception as e:
            logger.error(f"❌ Prediction error: {e}")
            return []

    def create_realtime_map(self):
        """Create real-time traffic map"""
        try:
            df = self.fetch_realtime_data()
            
            # Center map on Hamburg
            m = folium.Map(
                location=[53.5511, 9.9937], 
                zoom_start=11,
                tiles='OpenStreetMap'
            )
            
            if not df.empty:
                # Create heatmap
                heat_data = []
                for _, row in df.iterrows():
                    if pd.notna(row['lat']) and pd.notna(row['lon']):
                        weight = max(row['vehicle_count'], 1)
                        heat_data.append([row['lat'], row['lon'], weight])
                
                if heat_data:
                    HeatMap(
                        heat_data,
                        radius=25,
                        blur=20,
                        gradient={0.4: 'blue', 0.6: 'cyan', 0.8: 'yellow', 1.0: 'red'},
                        min_opacity=0.3
                    ).add_to(m)
                
                # Add markers for recent high-traffic areas
                high_traffic = df[df['vehicle_count'] > df['vehicle_count'].quantile(0.8)]
                for _, row in high_traffic.head(20).iterrows():
                    color = 'red' if row['vehicle_count'] > 15 else 'orange' if row['vehicle_count'] > 10 else 'yellow'
                    
                    folium.CircleMarker(
                        location=[row['lat'], row['lon']],
                        radius=8,
                        popup=f"""
                        <b>{row['zone_name']}</b><br>
                        🚗 Vehicles: {row['vehicle_count']}<br>
                        🏃 Speed: {row['speed_kmh']} km/h<br>
                        🕐 Time: {str(row['record_timestamp'])[:16]}
                        """,
                        color=color,
                        fill=True,
                        fillColor=color,
                        fillOpacity=0.7
                    ).add_to(m)
            else:
                # Add placeholder for empty data
                folium.Marker(
                    [53.5511, 9.9937],
                    popup="<b>Hamburg City Center</b><br>No recent traffic data",
                    icon=folium.Icon(color='blue', icon='info-sign')
                ).add_to(m)
            
            # Save map
            map_path = os.path.join('static', 'realtime_map.html')
            m.save(map_path)
            logger.info("✅ Real-time map created")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error creating real-time map: {e}")
            return False

    def create_prediction_map(self, hours_ahead=1):
        """Create prediction map for specified hours ahead"""
        try:
            predictions = self.predict_traffic(hours_ahead)
            
            # Center map on Hamburg
            m = folium.Map(
                location=[53.5511, 9.9937], 
                zoom_start=11,
                tiles='CartoDB positron'
            )
            
            if predictions:
                # Create heatmap for predictions
                heat_data = []
                for pred in predictions:
                    weight = pred['predicted_vehicles']
                    heat_data.append([pred['lat'], pred['lon'], weight])
                
                HeatMap(
                    heat_data,
                    radius=30,
                    blur=25,
                    gradient={0.4: 'green', 0.6: 'lime', 0.8: 'orange', 1.0: 'red'},
                    min_opacity=0.4
                ).add_to(m)
                
                # Add prediction markers
                for pred in predictions:
                    # Color based on predicted congestion
                    if pred['predicted_vehicles'] > 12:
                        color = 'red'
                        icon = 'exclamation-sign'
                    elif pred['predicted_vehicles'] > 8:
                        color = 'orange'
                        icon = 'warning-sign'
                    else:
                        color = 'green'
                        icon = 'ok-sign'
                    
                    folium.Marker(
                        location=[pred['lat'], pred['lon']],
                        popup=f"""
                        <b>🔮 {pred['zone']} Prediction</b><br>
                        🚗 Expected Vehicles: {pred['predicted_vehicles']}<br>
                        🏃 Expected Speed: {pred['predicted_speed']} km/h<br>
                        🕐 Time: +{hours_ahead}h from now<br>
                        📅 {pred['timestamp'][:16]}
                        """,
                        icon=folium.Icon(color=color, icon=icon)
                    ).add_to(m)
            
            # Save map
            map_path = os.path.join('static', 'prediction_map.html')
            m.save(map_path)
            logger.info(f"✅ Prediction map created for {hours_ahead}h ahead")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error creating prediction map: {e}")
            return False

# Initialize dashboard
dashboard = HamburgTrafficDashboard()

@app.route('/')
def index():
    """Main dashboard page"""
    try:
        # Create both maps
        dashboard.create_realtime_map()
        dashboard.create_prediction_map(hours_ahead=1)
        
        # Get current stats
        df = dashboard.fetch_realtime_data(limit=500)  # Increased limit
        stats = {
            'total_records': len(df),
            'active_zones': df['zone_name'].nunique() if not df.empty else 0,
            'avg_speed': round(df['speed_kmh'].mean(), 1) if not df.empty else 0,
            'avg_vehicles': round(df['vehicle_count'].mean(), 1) if not df.empty else 0,
            'last_update': datetime.now().strftime('%H:%M:%S'),
            'total_db_records': dashboard.get_total_records()
        }
        
        return render_template('dashboard.html', stats=stats)
        
    except Exception as e:
        logger.error(f"❌ Error in index route: {e}")
        return f"Error loading dashboard: {e}", 500

@app.route('/api/live-data')
def api_live_data():
    """API endpoint for live data stream"""
    try:
        # Get latest records from last 5 minutes
        query = """
        SELECT 
            RECORD_TIMESTAMP,
            LAT,
            LON,
            SPEED_KMH,
            VEHICLE_COUNT,
            ZONE_NAME,
            IS_MAJOR_ROAD,
            TRAFFIC_DENSITY
        FROM HAMBURG_TRAFFIC_LIVE 
        WHERE RECORD_TIMESTAMP > DATEADD(MINUTE, -5, CURRENT_TIMESTAMP())
        ORDER BY RECORD_TIMESTAMP DESC
        LIMIT 50
        """
        
        dashboard.cursor.execute(query)
        df = dashboard.cursor.fetch_pandas_all()
        
        if not df.empty:
            df.columns = df.columns.str.lower()
            
            # Convert to JSON-friendly format
            live_data = []
            for _, row in df.iterrows():
                live_data.append({
                    'timestamp': row['record_timestamp'].isoformat(),
                    'lat': float(row['lat']),
                    'lon': float(row['lon']),
                    'speed': int(row['speed_kmh']),
                    'vehicles': int(row['vehicle_count']),
                    'zone': row['zone_name'],
                    'is_major_road': bool(row['is_major_road'])
                })
            
            return jsonify({
                'status': 'success',
                'data': live_data,
                'count': len(live_data),
                'timestamp': datetime.now().isoformat()
            })
        else:
            return jsonify({
                'status': 'success',
                'data': [],
                'count': 0,
                'message': 'No recent data'
            })
            
    except Exception as e:
        logger.error(f"❌ Error in live-data API: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/stats-live')
def api_stats_live():
    """API endpoint for live statistics updates"""
    try:
        # Get recent data (last 10 minutes for better stats)
        df = dashboard.fetch_realtime_data(limit=1000)
        
        # Get total database count
        dashboard.cursor.execute("SELECT COUNT(*) as total FROM HAMBURG_TRAFFIC_LIVE")
        total_db = dashboard.cursor.fetchone()[0]
        
        if df.empty:
            return jsonify({
                'status': 'success',
                'stats': {
                    'total_records': 0,
                    'active_zones': 0,
                    'avg_speed': 0,
                    'avg_vehicles': 0,
                    'total_db_records': total_db,
                    'last_update': datetime.now().strftime('%H:%M:%S')
                }
            })
        
        # Calculate comprehensive stats
        stats = {
            'total_records': len(df),
            'active_zones': df['zone_name'].nunique(),
            'avg_speed': round(df['speed_kmh'].mean(), 1),
            'avg_vehicles': round(df['vehicle_count'].mean(), 1),
            'max_vehicles': int(df['vehicle_count'].max()),
            'min_speed': int(df['speed_kmh'].min()),
            'max_speed': int(df['speed_kmh'].max()),
            'busiest_zone': df.groupby('zone_name')['vehicle_count'].mean().idxmax(),
            'fastest_zone': df.groupby('zone_name')['speed_kmh'].mean().idxmax(),
            'slowest_zone': df.groupby('zone_name')['speed_kmh'].mean().idxmin(),
            'total_db_records': total_db,
            'last_update': datetime.now().strftime('%H:%M:%S'),
            'records_per_minute': round(len(df) / 10, 1)  # Records per minute in last 10 min
        }
        
        return jsonify({'status': 'success', 'stats': stats})
        
    except Exception as e:
        logger.error(f"❌ Error in stats-live API: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/recent-activity')
def api_recent_activity():
    """Get recent traffic activity for live feed"""
    try:
        query = """
        SELECT 
            RECORD_TIMESTAMP,
            ZONE_NAME,
            SPEED_KMH,
            VEHICLE_COUNT,
            IS_MAJOR_ROAD
        FROM HAMBURG_TRAFFIC_LIVE 
        WHERE RECORD_TIMESTAMP > DATEADD(MINUTE, -2, CURRENT_TIMESTAMP())
        ORDER BY RECORD_TIMESTAMP DESC
        LIMIT 20
        """
        
        dashboard.cursor.execute(query)
        df = dashboard.cursor.fetch_pandas_all()
        
        if not df.empty:
            df.columns = df.columns.str.lower()
            
            activities = []
            for _, row in df.iterrows():
                # Determine activity type
                if row['vehicle_count'] > 15:
                    activity_type = 'high_traffic'
                    icon = '🚗'
                elif row['speed_kmh'] < 15:
                    activity_type = 'slow_traffic'
                    icon = '🐌'
                elif row['is_major_road']:
                    activity_type = 'highway'
                    icon = '🛣️'
                else:
                    activity_type = 'normal'
                    icon = '🚙'
                
                activities.append({
                    'timestamp': row['record_timestamp'].strftime('%H:%M:%S'),
                    'zone': row['zone_name'],
                    'speed': int(row['speed_kmh']),
                    'vehicles': int(row['vehicle_count']),
                    'type': activity_type,
                    'icon': icon,
                    'message': f"{icon} {row['zone_name']}: {row['vehicle_count']} vehicles @ {row['speed_kmh']}km/h"
                })
            
            return jsonify({
                'status': 'success',
                'activities': activities
            })
        else:
            return jsonify({
                'status': 'success',
                'activities': []
            })
            
    except Exception as e:
        logger.error(f"❌ Error in recent-activity API: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/predict/<int:hours>')
def api_predict(hours):
    """API endpoint for dynamic predictions"""
    try:
        # Validate hours (1-5 hours max)
        hours = max(1, min(hours, 5))
        
        # Create prediction map
        success = dashboard.create_prediction_map(hours_ahead=hours)
        
        if success:
            predictions = dashboard.predict_traffic(hours_ahead=hours)
            return jsonify({
                'status': 'success',
                'hours_ahead': hours,
                'predictions': predictions,
                'timestamp': datetime.now().isoformat()
            })
        else:
            return jsonify({'status': 'error', 'message': 'Failed to generate predictions'}), 500
            
    except Exception as e:
        logger.error(f"❌ Error in predict API: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/stats')
def api_stats():
    """API endpoint for current statistics"""
    try:
        df = dashboard.fetch_realtime_data()
        
        if df.empty:
            return jsonify({
                'status': 'success',
                'message': 'No recent data',
                'stats': {
                    'total_records': 0,
                    'active_zones': 0,
                    'avg_speed': 0,
                    'avg_vehicles': 0
                }
            })
        
        # Calculate detailed stats
        stats = {
            'total_records': len(df),
            'active_zones': df['zone_name'].nunique(),
            'avg_speed': round(df['speed_kmh'].mean(), 1),
            'avg_vehicles': round(df['vehicle_count'].mean(), 1),
            'max_vehicles': int(df['vehicle_count'].max()),
            'min_speed': int(df['speed_kmh'].min()),
            'max_speed': int(df['speed_kmh'].max()),
            'busiest_zone': df.groupby('zone_name')['vehicle_count'].mean().idxmax(),
            'fastest_zone': df.groupby('zone_name')['speed_kmh'].mean().idxmax(),
            'last_update': datetime.now().strftime('%H:%M:%S')
        }
        
        return jsonify({'status': 'success', 'stats': stats})
        
    except Exception as e:
        logger.error(f"❌ Error in stats API: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/refresh')
def api_refresh():
    """API endpoint to refresh maps"""
    try:
        # Refresh both maps
        realtime_success = dashboard.create_realtime_map()
        prediction_success = dashboard.create_prediction_map(hours_ahead=1)
        
        return jsonify({
            'status': 'success',
            'realtime_updated': realtime_success,
            'prediction_updated': prediction_success,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"❌ Error in refresh API: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

if __name__ == '__main__':
    # Ensure static directory exists
    os.makedirs('static', exist_ok=True)
    
    # Run the app
    logger.info("🚀 Starting Hamburg Traffic Dashboard with Real-time Updates...")
    app.run(debug=True, host='0.0.0.0', port=8082)