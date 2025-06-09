import pandas as pd
import numpy as np
import snowflake.connector
from xgboost import XGBRegressor
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.metrics import mean_squared_error, r2_score, mean_absolute_error
from sklearn.preprocessing import LabelEncoder, StandardScaler
import joblib
import json
import logging
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class HamburgTrafficPredictor:
    def __init__(self, config_path="snowflake_config.json"):
        """Initialize the Hamburg Traffic Predictor"""
        self.config_path = config_path
        self.load_config()
        self.setup_snowflake_connection()
        
        # Feature encoders
        self.zone_encoder = LabelEncoder()
        self.scaler = StandardScaler()
        
        # Hamburg zone traffic characteristics (for feature engineering)
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
            'Winterhude': {'base_traffic': 0.6, 'rush_multiplier': 1.4, 'weekend_factor': 0.6},
            'A1_North': {'base_traffic': 0.8, 'rush_multiplier': 2.0, 'weekend_factor': 0.4},
            'A7_West': {'base_traffic': 0.8, 'rush_multiplier': 1.9, 'weekend_factor': 0.4},
            'B4_Central': {'base_traffic': 0.7, 'rush_multiplier': 1.7, 'weekend_factor': 0.5},
            'Ring_3': {'base_traffic': 0.6, 'rush_multiplier': 1.6, 'weekend_factor': 0.4},
            'Elbchaussee': {'base_traffic': 0.5, 'rush_multiplier': 1.3, 'weekend_factor': 0.8}
        }
        
        self.model = None
        logger.info("✅ Hamburg Traffic Predictor initialized")

    def load_config(self):
        """Load Snowflake configuration"""
        with open(self.config_path, "r") as f:
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

    def fetch_training_data(self, hours_back=168):  # 1 week of data
        """Fetch and prepare training data from Snowflake"""
        logger.info(f"📊 Fetching last {hours_back} hours of traffic data...")
        
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
        WHERE RECORD_TIMESTAMP >= DATEADD(HOUR, -{hours_back}, CURRENT_TIMESTAMP())
        ORDER BY RECORD_TIMESTAMP
        """
        
        try:
            self.cursor.execute(query)
            df = self.cursor.fetch_pandas_all()
            
            # Convert column names to lowercase for consistency
            df.columns = df.columns.str.lower()
            logger.info(f"✅ Retrieved {len(df)} records for training")
            
            if df.empty:
                logger.warning("⚠️ No data found! Generating synthetic data for demo...")
                df = self.generate_synthetic_data()
            
            return df
        except Exception as e:
            logger.error(f"❌ Error fetching data: {e}")
            logger.info("📝 Generating synthetic data for demo...")
            return self.generate_synthetic_data()

    def generate_synthetic_data(self, n_records=5000):
        """Generate synthetic Hamburg traffic data for training if DB is empty"""
        logger.info(f"🔧 Generating {n_records} synthetic training records...")
        
        zones = list(self.zone_characteristics.keys())
        base_time = datetime.now() - timedelta(hours=168)  # 1 week ago
        
        data = []
        for i in range(n_records):
            timestamp = base_time + timedelta(minutes=i*2)  # Every 2 minutes
            zone = np.random.choice(zones)
            
            # Get zone characteristics
            zone_chars = self.zone_characteristics[zone]
            
            # Time-based features
            hour = timestamp.hour
            day_of_week = timestamp.weekday()
            is_weekend = day_of_week >= 5
            is_rush_hour = hour in [7, 8, 17, 18, 19]
            
            # Generate realistic traffic based on time and zone
            base_vehicles = zone_chars['base_traffic'] * 15
            if is_rush_hour:
                base_vehicles *= zone_chars['rush_multiplier']
            if is_weekend:
                base_vehicles *= zone_chars['weekend_factor']
            
            vehicle_count = max(1, int(base_vehicles * np.random.uniform(0.7, 1.3)))
            
            # Speed inversely related to vehicle count and zone density
            if vehicle_count > 12:
                speed = np.random.randint(5, 25)
            elif vehicle_count > 8:
                speed = np.random.randint(15, 40)
            else:
                speed = np.random.randint(25, 70)
            
            # Coordinates (simplified for synthetic data)
            lat = 53.5511 + np.random.uniform(-0.05, 0.05)
            lon = 9.9937 + np.random.uniform(-0.05, 0.05)
            
            data.append({
                'record_timestamp': timestamp,
                'lat': lat,
                'lon': lon,
                'speed_kmh': speed,
                'vehicle_count': vehicle_count,
                'zone_name': zone,
                'is_major_road': 'A' in zone or 'B' in zone or 'Ring' in zone,
                'traffic_density': zone_chars['base_traffic']
            })
        
        df = pd.DataFrame(data)
        logger.info("✅ Synthetic data generated successfully")
        return df

    def engineer_features(self, df):
        """Create enhanced features for better predictions"""
        logger.info("🔧 Engineering enhanced features...")
        
        # Make a copy to avoid modifying original
        df = df.copy()
        
        # Convert timestamp
        df['record_timestamp'] = pd.to_datetime(df['record_timestamp'])
        
        # Time-based features
        df['hour'] = df['record_timestamp'].dt.hour
        df['day_of_week'] = df['record_timestamp'].dt.dayofweek
        df['month'] = df['record_timestamp'].dt.month
        df['is_weekend'] = (df['day_of_week'] >= 5).astype(int)
        
        # Rush hour patterns
        df['is_morning_rush'] = ((df['hour'] >= 7) & (df['hour'] <= 9)).astype(int)
        df['is_evening_rush'] = ((df['hour'] >= 17) & (df['hour'] <= 19)).astype(int)
        df['is_lunch_time'] = ((df['hour'] >= 12) & (df['hour'] <= 14)).astype(int)
        df['is_night'] = ((df['hour'] <= 6) | (df['hour'] >= 22)).astype(int)
        
        # Zone-based features
        df['zone_encoded'] = self.zone_encoder.fit_transform(df['zone_name'].fillna('Unknown'))
        
        # Add zone characteristics
        df['zone_base_traffic'] = df['zone_name'].map(
            lambda x: self.zone_characteristics.get(x, {'base_traffic': 0.5})['base_traffic']
        )
        df['zone_rush_multiplier'] = df['zone_name'].map(
            lambda x: self.zone_characteristics.get(x, {'rush_multiplier': 1.0})['rush_multiplier']
        )
        df['zone_weekend_factor'] = df['zone_name'].map(
            lambda x: self.zone_characteristics.get(x, {'weekend_factor': 0.5})['weekend_factor']
        )
        
        # Interaction features
        df['rush_zone_interaction'] = (df['is_morning_rush'] + df['is_evening_rush']) * df['zone_rush_multiplier']
        df['weekend_zone_interaction'] = df['is_weekend'] * df['zone_weekend_factor']
        
        # Speed categories
        df['speed_category'] = pd.cut(df['speed_kmh'], 
                                    bins=[0, 15, 30, 50, 100], 
                                    labels=['very_slow', 'slow', 'medium', 'fast'])
        df['speed_category'] = LabelEncoder().fit_transform(df['speed_category'])
        
        # Vehicle density
        df['vehicle_density'] = df['vehicle_count'] / (df['traffic_density'] + 0.1)
        
        # Lag features (previous hour traffic)
        df_sorted = df.sort_values('record_timestamp')
        df_sorted['prev_hour_vehicles'] = df_sorted.groupby('zone_name')['vehicle_count'].shift(30)  # ~1 hour ago
        df_sorted['prev_hour_speed'] = df_sorted.groupby('zone_name')['speed_kmh'].shift(30)
        
        # Fill NaN values
        df_sorted['prev_hour_vehicles'] = df_sorted['prev_hour_vehicles'].fillna(df_sorted['vehicle_count'].mean())
        df_sorted['prev_hour_speed'] = df_sorted['prev_hour_speed'].fillna(df_sorted['speed_kmh'].mean())
        
        # Traffic trend
        df_sorted['traffic_trend'] = df_sorted['vehicle_count'] - df_sorted['prev_hour_vehicles']
        
        logger.info(f"✅ Feature engineering complete. Shape: {df_sorted.shape}")
        return df_sorted

    def prepare_features_and_targets(self, df):
        """Prepare feature matrix and target variables"""
        feature_columns = [
            'hour', 'day_of_week', 'month', 'is_weekend',
            'is_morning_rush', 'is_evening_rush', 'is_lunch_time', 'is_night',
            'zone_encoded', 'zone_base_traffic', 'zone_rush_multiplier', 'zone_weekend_factor',
            'rush_zone_interaction', 'weekend_zone_interaction',
            'speed_category', 'vehicle_density', 'traffic_density',
            'prev_hour_vehicles', 'prev_hour_speed', 'traffic_trend',
            'lat', 'lon', 'is_major_road'
        ]
        
        # Ensure all feature columns exist
        missing_cols = [col for col in feature_columns if col not in df.columns]
        if missing_cols:
            logger.warning(f"⚠️ Missing columns: {missing_cols}")
            for col in missing_cols:
                df[col] = 0
        
        X = df[feature_columns].copy()
        
        # Multiple prediction targets
        y_vehicles = df['vehicle_count'].copy()
        y_speed = df['speed_kmh'].copy()
        
        # Handle missing values
        X = X.fillna(X.mean())
        
        logger.info(f"✅ Features prepared: {X.shape}, Targets: {len(y_vehicles)}")
        return X, y_vehicles, y_speed

    def train_models(self, X, y_vehicles, y_speed):
        """Train XGBoost models for vehicle count and speed prediction"""
        logger.info("🧠 Training enhanced XGBoost models...")
        
        # Split data
        X_train, X_test, y_veh_train, y_veh_test, y_speed_train, y_speed_test = train_test_split(
            X, y_vehicles, y_speed, test_size=0.2, random_state=42
        )
        
        # Scale features
        X_train_scaled = self.scaler.fit_transform(X_train)
        X_test_scaled = self.scaler.transform(X_test)
        
        # Vehicle Count Model
        logger.info("🚗 Training vehicle count predictor...")
        self.vehicle_model = XGBRegressor(
            n_estimators=200,
            max_depth=8,
            learning_rate=0.1,
            subsample=0.8,
            colsample_bytree=0.8,
            random_state=42,
            n_jobs=-1
        )
        self.vehicle_model.fit(X_train_scaled, y_veh_train)
        
        # Speed Model
        logger.info("🏃 Training speed predictor...")
        self.speed_model = XGBRegressor(
            n_estimators=200,
            max_depth=8,
            learning_rate=0.1,
            subsample=0.8,
            colsample_bytree=0.8,
            random_state=42,
            n_jobs=-1
        )
        self.speed_model.fit(X_train_scaled, y_speed_train)
        
        # Evaluate models
        self.evaluate_models(X_test_scaled, y_veh_test, y_speed_test)
        
        return X_train, X_test, y_veh_test, y_speed_test

    def evaluate_models(self, X_test, y_veh_test, y_speed_test):
        """Evaluate model performance"""
        logger.info("📊 Evaluating model performance...")
        
        # Vehicle predictions
        veh_pred = self.vehicle_model.predict(X_test)
        veh_mse = mean_squared_error(y_veh_test, veh_pred)
        veh_mae = mean_absolute_error(y_veh_test, veh_pred)
        veh_r2 = r2_score(y_veh_test, veh_pred)
        
        # Speed predictions
        speed_pred = self.speed_model.predict(X_test)
        speed_mse = mean_squared_error(y_speed_test, speed_pred)
        speed_mae = mean_absolute_error(y_speed_test, speed_pred)
        speed_r2 = r2_score(y_speed_test, speed_pred)
        
        # Log results
        logger.info("🚗 Vehicle Count Model Performance:")
        logger.info(f"   MSE: {veh_mse:.2f}, MAE: {veh_mae:.2f}, R²: {veh_r2:.3f}")
        
        logger.info("🏃 Speed Model Performance:")
        logger.info(f"   MSE: {speed_mse:.2f}, MAE: {speed_mae:.2f}, R²: {speed_r2:.3f}")
        
        # Feature importance
        self.log_feature_importance()

    def log_feature_importance(self):
        """Log feature importance for both models"""
        if hasattr(self, 'vehicle_model'):
            veh_importance = self.vehicle_model.feature_importances_
            logger.info("🔍 Top 5 Vehicle Count Features:")
            for i, importance in enumerate(sorted(enumerate(veh_importance), key=lambda x: x[1], reverse=True)[:5]):
                logger.info(f"   Feature {importance[0]}: {importance[1]:.3f}")

    def predict_future_traffic(self, hours_ahead=5):
        """Predict traffic for the next N hours"""
        logger.info(f"🔮 Predicting traffic for next {hours_ahead} hours...")
        
        predictions = []
        current_time = datetime.now()
        
        for zone in self.zone_characteristics.keys():
            for hour_offset in range(1, hours_ahead + 1):
                future_time = current_time + timedelta(hours=hour_offset)
                
                # Create feature vector for prediction
                features = self.create_prediction_features(zone, future_time)
                features_scaled = self.scaler.transform([features])
                
                # Predict
                predicted_vehicles = max(1, int(self.vehicle_model.predict(features_scaled)[0]))
                predicted_speed = max(5, int(self.speed_model.predict(features_scaled)[0]))
                
                predictions.append({
                    'timestamp': future_time,
                    'zone': zone,
                    'predicted_vehicles': predicted_vehicles,
                    'predicted_speed': predicted_speed,
                    'hours_ahead': hour_offset
                })
        
        return pd.DataFrame(predictions)

    def create_prediction_features(self, zone, timestamp):
        """Create feature vector for a specific zone and time"""
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

    def save_models(self, path_prefix="webapp/"):
        """Save trained models and encoders"""
        logger.info(f"💾 Saving models to {path_prefix}...")
        
        # Save models
        joblib.dump(self.vehicle_model, f"{path_prefix}vehicle_model.pkl")
        joblib.dump(self.speed_model, f"{path_prefix}speed_model.pkl")
        joblib.dump(self.zone_encoder, f"{path_prefix}zone_encoder.pkl")
        joblib.dump(self.scaler, f"{path_prefix}scaler.pkl")
        
        # Save zone characteristics
        with open(f"{path_prefix}zone_characteristics.json", 'w') as f:
            json.dump(self.zone_characteristics, f)
        
        logger.info("✅ Models saved successfully")

    def run_training_pipeline(self):
        """Run the complete training pipeline"""
        logger.info("🚀 Starting Hamburg Traffic Prediction Training...")
        
        try:
            # Step 1: Fetch data
            df = self.fetch_training_data()
            
            # Step 2: Engineer features
            df_features = self.engineer_features(df)
            
            # Step 3: Prepare features and targets
            X, y_vehicles, y_speed = self.prepare_features_and_targets(df_features)
            
            # Step 4: Train models
            self.train_models(X, y_vehicles, y_speed)
            
            # Step 5: Generate sample predictions
            predictions = self.predict_future_traffic(hours_ahead=5)
            logger.info(f"🔮 Sample predictions generated for {len(predictions)} zone-hour combinations")
            
            # Step 6: Save models
            self.save_models()
            
            logger.info("🎉 Training pipeline completed successfully!")
            
            return predictions
            
        except Exception as e:
            logger.error(f"❌ Training pipeline failed: {e}")
            raise
        finally:
            if hasattr(self, 'conn'):
                self.conn.close()

if __name__ == "__main__":
    predictor = HamburgTrafficPredictor()
    predictions = predictor.run_training_pipeline()
    
    # Show sample predictions
    print("\n🔮 Sample Future Traffic Predictions:")
    print(predictions.head(10))