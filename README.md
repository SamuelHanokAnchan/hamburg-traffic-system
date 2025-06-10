# Hamburg Traffic Monitoring & Prediction System

A real-time traffic monitoring and prediction system for Hamburg, Germany that combines data streaming, machine learning, and interactive visualization to provide traffic insights and forecasts.

## 🚀 Features

- **Real-time Traffic Monitoring**: Live traffic speed and vehicle count tracking
- **Predictive Analytics**: Machine learning-powered traffic prediction using XGBoost
- **Interactive Maps**: Dynamic Folium-based visualizations showing traffic conditions
- **Streaming Architecture**: Kafka-based data pipeline for real-time processing
- **Cloud Integration**: Snowflake for data warehousing and Google BigQuery support
- **Web Dashboard**: Flask-based web application for data visualization

## 🏗️ Architecture

The system consists of several interconnected components:

```
Data Sources → Kafka → Snowflake/BigQuery → ML Model → Web Dashboard
```

- **Data Producer**: Generates simulated traffic data using Faker
- **Kafka Stream**: Real-time data ingestion and processing
- **Data Warehouse**: Snowflake for storing historical and live traffic data
- **ML Pipeline**: XGBoost model for traffic prediction
- **Web Application**: Flask app with interactive maps

## 📋 Prerequisites

- Python 3.8+
- Docker & Docker Compose
- Snowflake account (or Google Cloud Platform for BigQuery)
- Git

## 🛠️ Installation

### 1. Clone the Repository

```bash
git clone <repository-url>
cd hamburg-traffic-system
```

### 2. Install Python Dependencies

```bash
pip install -r requirements.txt
```

Required packages:
```python
pandas
flask
folium
snowflake-connector-python
kafka-python
faker
xgboost
scikit-learn
joblib
google-cloud-bigquery
schedule
```

### 3. Set Up Kafka Infrastructure

```bash
docker-compose up -d
```

This will start Zookeeper and Kafka services.

### 4. Configure Data Connections

#### Snowflake Configuration

Create a `snowflake_config.json` file:

```json
{
  "user": "your_username",
  "password": "your_password", 
  "account": "your_account",
  "warehouse": "TRAFFIC_WH",
  "database": "TRAFFIC_DB",
  "schema": "TRAFFIC_SCHEMA"
}
```

#### Google Cloud Configuration (Optional)

For BigQuery integration, set up your `gcp_key.json` credentials file.

```bash
python setup_bigquery.py
```

## 🚦 Usage

### 1. Start Data Streaming

Generate and stream simulated traffic data:

```bash
python kafka/traffic_producer.py
```

### 2. Train the ML Model

Train the XGBoost prediction model:

```bash
python train_traffic_model.py
```

This will:
- Fetch historical data from Snowflake
- Train an XGBoost regression model
- Save the model as `traffic_model.pkl`

### 3. Run Data Processing

Process real-time data and generate predictions:

```bash
python webapp/check.py
```

This script will:
- 🔗 Connect to Snowflake
- 🚦 Fetch real-time traffic data
- 🧠 Load the trained ML model
- 📊 Generate traffic predictions
- 🗺️ Create interactive maps

### 4. Launch Web Application

Start the Flask web dashboard:

```bash
cd webapp
python app.py
```

Access the dashboard at `http://localhost:8081`

## 📊 Data Schema

### Live Stream Table
```sql
CREATE TABLE LIVE_STREAM (
    TIMESTAMP TIMESTAMP,
    LATITUDE FLOAT,
    LONGITUDE FLOAT, 
    SPEED INTEGER,
    VEHICLE_COUNT INTEGER
);
```

### Hourly Summary Table
```sql
CREATE TABLE HOURLY_SUMMARY (
    HOUR TIMESTAMP,
    TOTAL_VEHICLES INTEGER,
    AVG_SPEED FLOAT
);
```

## 🤖 Machine Learning Model

The system uses **XGBoost Regression** for traffic prediction with the following features:

- **hour_of_day**: Hour of the day (0-23)
- **day_of_week**: Day of week (0-6, Monday=0)

**Target Variable**: `TOTAL_VEHICLES` - Predicted number of vehicles

### Model Performance
The model is evaluated using Mean Squared Error (MSE) on test data.

## 📁 Project Structure

```
hamburg-traffic-system/
├── kafka/
│   └── traffic_producer.py      # Kafka data producer
├── webapp/
│   ├── app.py                   # Flask web application
│   ├── check.py                 # Data processing script
│   ├── static/                  # Generated maps
│   └── templates/               # HTML templates
├── data/
│   ├── traffic_data.csv         # Historical traffic data
│   └── traffic_map.html         # Traffic visualization
├── ml/
│   └── predicted_traffic.csv    # ML predictions output
├── docker-compose.yml           # Kafka infrastructure
├── setup_bigquery.py           # BigQuery setup script
├── train_traffic_model.py       # ML model training
└── README.md                    # This file
```

## 🗺️ Interactive Maps

The system generates two types of interactive maps:

1. **Real-time Traffic Map** (`realtime_map.html`)
   - Shows current traffic speeds
   - Blue markers indicate speed data points
   - Updates with live data from Snowflake

2. **Prediction Map** (`predicted_map.html`)
   - Displays ML-generated traffic predictions
   - Green markers show predicted vehicle counts
   - Based on time-based features

## ⚙️ Configuration

### Environment Variables

- `GOOGLE_APPLICATION_CREDENTIALS`: Path to GCP service account key
- Snowflake credentials in `snowflake_config.json`

### Kafka Settings

- Bootstrap servers: `localhost:9092`
- Topic: `hamburg_traffic`
- Zookeeper: `localhost:2181`

## 🐛 Troubleshooting

### Common Issues

1. **Kafka Connection Error**
   ```bash
   docker-compose ps  # Check if Kafka is running
   docker-compose restart kafka
   ```

2. **Snowflake Authentication**
   - Verify credentials in `snowflake_config.json`
   - Check warehouse, database, and schema names

3. **Model Not Found**
   ```bash
   python train_traffic_model.py  # Retrain the model
   ```

4. **Empty Data**
   - Ensure Kafka producer is running
   - Check if data is flowing to Snowflake tables

## 🔄 Recent Updates

Based on the latest commit, the system includes:

- Enhanced error handling in data processing scripts
- Improved map generation with better visualization
- Updated model prediction pipeline
- Streamlined web application with better error messages

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🙏 Acknowledgments

- Hamburg city traffic data sources
- Kafka ecosystem for real-time streaming
- Snowflake for cloud data warehousing
- XGBoost for machine learning capabilities
- Folium for interactive map visualizations

---

**Note**: This system is designed for demonstration and research purposes. For production deployment, ensure proper security measures, error handling, and monitoring are implemented.