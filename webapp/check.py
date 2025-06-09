import pandas as pd
import folium
import joblib
from snowflake.connector import connect
import json
from datetime import datetime

# Load Snowflake config
with open("snowflake_config.json") as f:
    SNOWFLAKE_CONFIG = json.load(f)

REALTIME_QUERY = "SELECT TIMESTAMP, LATITUDE, LONGITUDE, SPEED FROM LIVE_STREAM ORDER BY TIMESTAMP DESC LIMIT 1"
PREDICTION_QUERY = "SELECT TIMESTAMP FROM LIVE_STREAM ORDER BY TIMESTAMP DESC LIMIT 1"

def fetch_data(query):
    print("🔗 Connecting to Snowflake...")
    conn = connect(**SNOWFLAKE_CONFIG)
    cs = conn.cursor()
    try:
        cs.execute(query)
        df = cs.fetch_pandas_all()
        print(f"✅ Query returned {len(df)} rows")
        return df
    finally:
        cs.close()
        conn.close()

def generate_map(df, filename, value_column):
    print(f"🗺️ Generating map for {filename} with {len(df)} rows...")
    if df.empty:
        print("⚠️ No data to show on map.")
        return
    m = folium.Map(location=[df["LATITUDE"].mean(), df["LONGITUDE"].mean()], zoom_start=13)
    for _, row in df.iterrows():
        folium.CircleMarker(
            location=[row["LATITUDE"], row["LONGITUDE"]],
            radius=6,
            color="blue" if value_column == "SPEED" else "green",
            fill=True,
            fill_opacity=0.6,
            popup=f"{value_column}: {row[value_column]}"
        ).add_to(m)
    m.save(filename)
    print(f"✅ Map saved: {filename}")

def main():
    print("🚦 Fetching real-time traffic data...")
    real_df = fetch_data(REALTIME_QUERY)
    if not real_df.empty:
        generate_map(real_df, "webapp/static/realtime_map.html", "SPEED")

    print("\n📊 Fetching input data for prediction...")
    pred_df = fetch_data(PREDICTION_QUERY)
    if pred_df.empty:
        print("⚠️ No data available for prediction.")
        return

    # Extract features expected by model
    pred_df["hour_of_day"] = pd.to_datetime(pred_df["TIMESTAMP"]).dt.hour
    pred_df["day_of_week"] = pd.to_datetime(pred_df["TIMESTAMP"]).dt.dayofweek
    pred_features = pred_df[["hour_of_day", "day_of_week"]]

    print("🧠 Loading model...")
    try:
        model = joblib.load("webapp/model.pkl")
        print("✅ Model loaded successfully.")
    except FileNotFoundError:
        print("❌ Model file not found at 'webapp/model.pkl'")
        return

    try:
        pred_df["PREDICTED_VEHICLES"] = model.predict(pred_features)
    except Exception as e:
        print(f"❌ Prediction failed: {e}")
        return

    # Dummy coordinates for visualization
    pred_df["LATITUDE"] = 53.55
    pred_df["LONGITUDE"] = 9.99
    generate_map(pred_df, "webapp/static/predicted_map.html", "PREDICTED_VEHICLES")

if __name__ == "__main__":
    main()
