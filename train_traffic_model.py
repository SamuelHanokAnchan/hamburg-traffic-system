import pandas as pd
import snowflake.connector
from xgboost import XGBRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
import joblib
import json

# Load Snowflake credentials
with open("snowflake_config.json", "r") as f:
    config = json.load(f)

# Step 1: Fetch data from Snowflake
def fetch_data():
    conn = snowflake.connector.connect(
        user=config["user"],
        password=config["password"],
        account=config["account"],
        warehouse=config["warehouse"],
        database=config["database"],
        schema=config["schema"]
    )
    cs = conn.cursor()
    try:
        cs.execute("SELECT * FROM HOURLY_SUMMARY")
        df = cs.fetch_pandas_all()
    finally:
        cs.close()
        conn.close()
    return df

# Step 2: Prepare features
def prepare_data(df):
    df['HOUR'] = pd.to_datetime(df['HOUR'])
    df['hour_of_day'] = df['HOUR'].dt.hour
    df['day_of_week'] = df['HOUR'].dt.dayofweek
    X = df[['hour_of_day', 'day_of_week']]
    y = df['TOTAL_VEHICLES']
    return train_test_split(X, y, test_size=0.2, random_state=42)

# Step 3: Train model
def train_model(X_train, y_train):
    model = XGBRegressor()
    model.fit(X_train, y_train)
    return model

# Step 4: Evaluate model
def evaluate_model(model, X_test, y_test):
    y_pred = model.predict(X_test)
    mse = mean_squared_error(y_test, y_pred)
    print(f"✅ Mean Squared Error: {mse:.2f}")
    print("🔍 Sample Predictions:", y_pred[:5])

# Step 5: Save model
def save_model(model, path="traffic_model.pkl"):
    joblib.dump(model, path)
    print(f"💾 Model saved to {path}")

# === MAIN ===
def main():
    print("🚦 Loading traffic data from Snowflake...")
    df = fetch_data()
    if df.empty:
        print("❌ No data found in HOURLY_SUMMARY.")
        return

    print("📊 Preparing features...")
    X_train, X_test, y_train, y_test = prepare_data(df)

    print("🧠 Training XGBoost model...")
    model = train_model(X_train, y_train)

    print("📈 Evaluating model performance...")
    evaluate_model(model, X_test, y_test)

    print("💾 Saving model to disk...")
    save_model(model)

if __name__ == "__main__":
    main()
