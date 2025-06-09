from flask import Flask, render_template
import snowflake.connector
import folium
import joblib
import pandas as pd
from datetime import datetime

app = Flask(__name__)

SNOWFLAKE_CONFIG = {
    'user': 'SamuelHanokAnchan',
    'password': 'Samuel_hanok02',
    'account': 'pxwrgft-aq51530',
    'warehouse': 'TRAFFIC_WH',
    'database': 'TRAFFIC_DB',
    'schema': 'TRAFFIC_SCHEMA'
}

REALTIME_QUERY = """
SELECT TIMESTAMP, 53.5511 AS LATITUDE, 9.9937 AS LONGITUDE, SPEED
FROM LIVE_STREAM
WHERE TIMESTAMP > DATEADD(MINUTE, -60, CURRENT_TIMESTAMP())
ORDER BY TIMESTAMP DESC
LIMIT 100
"""

PREDICTION_QUERY = """
SELECT HOUR, TOTAL_VEHICLES, AVG_SPEED
FROM HOURLY_SUMMARY
ORDER BY HOUR DESC
LIMIT 24
"""

def fetch_data(query):
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cs = conn.cursor()
    try:
        cs.execute(query)
        df = cs.fetch_pandas_all()
    finally:
        cs.close()
        conn.close()
    return df

def create_map(df, filename):
    if df.empty:
        raise ValueError("No data to plot on map")

    m = folium.Map(location=[53.5511, 9.9937], zoom_start=12)

    for _, row in df.iterrows():
        folium.CircleMarker(
            location=[row['LATITUDE'], row['LONGITUDE']],
            radius=7,
            popup=f"Speed: {row['SPEED']} km/h",
            color='blue',
            fill=True,
            fill_color='blue'
        ).add_to(m)

    m.save(filename)

@app.route('/')
def index():
    try:
        real_df = fetch_data(REALTIME_QUERY)
        create_map(real_df, 'webapp/static/realtime_map.html')
    except Exception as e:
        return f"Error generating real-time map: {e}"

    try:
        pred_df = fetch_data(PREDICTION_QUERY)
        # For simplicity, just create a static map centered similarly, no prediction points
        pred_map = folium.Map(location=[53.5511, 9.9937], zoom_start=12)
        pred_map.save('webapp/static/prediction_map.html')
    except Exception as e:
        return f"Error generating prediction map: {e}"

    return render_template('index.html')

if __name__ == '__main__':
    app.run(debug=True, port=8081)
