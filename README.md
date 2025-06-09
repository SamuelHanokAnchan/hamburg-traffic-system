# Hamburg Traffic System

## Project Overview

This project implements a real-time traffic monitoring and prediction system for Hamburg city. It combines batch processing, streaming data ingestion, machine learning-based traffic forecasting, and an interactive web dashboard displaying traffic maps.

### Key Components

- **Data Ingestion & Storage:** Real-time traffic data is ingested and stored using Kafka and Snowflake.
- **Batch Processing:** Airflow pipelines summarize hourly traffic data from raw streams.
- **Machine Learning:** An XGBoost regression model predicts future traffic volumes using historical data.
- **Web Application:** A Flask-based dashboard visualizes real-time and predicted traffic on side-by-side interactive maps.

---

## What Works

- **Real-time Data Streaming:** Kafka setup and ingestion of synthetic or real traffic streams.
- **Airflow DAG:** Scheduled batch jobs run to summarize traffic hourly and load data into Snowflake.
- **ML Model Training:** The traffic prediction model is trained using a generated dataset.
- **Flask UI:** Displays two maps side-by-side using Folium — one for real-time traffic and one for predicted traffic.
- **Basic Snowflake Integration:** Data reads and writes are operational for most parts.

---

## Known Issues & Limitations

- **Batch Processing (Airflow) Issues:**  
  The batch summarization DAG intermittently fails or does not process data correctly due to schema mismatches or data availability issues in Snowflake tables. The `HOURLY_SUMMARY` table sometimes lacks timely updates.

- **Training Data:**  
  The machine learning model was trained primarily on synthetic or incomplete data due to batch processing gaps, limiting prediction accuracy.

- **Final Map Visualization:**  
  Although the Flask app runs without errors, the predicted traffic map does not render as expected on the UI. This is likely due to data format mismatches or rendering issues in Folium integration.

- **Snowflake SQL Errors:**  
  Occasional SQL compilation errors related to invalid column identifiers (`LATITUDE`, `TIMESTAMP`, etc.) suggest schema discrepancies between Snowflake tables and the queries in the codebase.

- **Port Conflicts & Environment:**  
  Docker containers for Kafka, Airflow, and web app sometimes face port conflicts or dependency issues affecting startup.

---

## Setup & Usage Instructions

1. **Clone the repository**

   ```bash
   git clone https://github.com/<username>/hamburg-traffic-system.git
   cd hamburg-traffic-system
Docker Compose

Run Kafka, Zookeeper, Postgres, Airflow, and other services:


docker-compose up --build
Airflow

Initialize the Airflow database before first run:

docker exec -it airflow-airflow-webserver-1 airflow db init
Access Airflow UI at http://localhost:8081

Snowflake Setup

Configure Snowflake credentials in snowflake_config.json.

Ensure database, schema, and tables exist as per the SQL scripts.

Machine Learning Model

Train the model using train_traffic_model.py.

Save the trained model as model.pkl in the webapp directory.

Web Application

Navigate to webapp directory.

Run Flask app:

bash
Copy
python app.py
Access the UI at http://localhost:8081

Next Steps / Recommendations
Fix Batch Processing:
Debug and fix Airflow DAGs to ensure reliable data aggregation in Snowflake.

Use Real Batch Data for Training:
Retrain ML models on real summarized traffic data for better prediction quality.

Debug Map Rendering:
Investigate Folium map generation logic and ensure correct data formatting.

Schema Consistency:
Align Snowflake table schemas with code SQL queries to avoid compilation errors.

Automate Deployment:
Set up CI/CD pipelines for smoother development and deployment cycles.

Contact
For questions or collaboration, contact Samuel Hanok Anchan.

License
Specify your license here (e.g., MIT License).

yaml
Copy

---

Would you like me to generate any additional documentation or a detailed troubleshooting guide?





You said:
give me in one single read me file


ChatGPT said:
Certainly! Here’s the entire README.md content in one single file format for easy copy-pasting:

markdown
Copy
# Hamburg Traffic System

## Project Overview

This project implements a real-time traffic monitoring and prediction system for Hamburg city. It combines batch processing, streaming data ingestion, machine learning-based traffic forecasting, and an interactive web dashboard displaying traffic maps.

### Key Components

- **Data Ingestion & Storage:** Real-time traffic data is ingested and stored using Kafka and Snowflake.
- **Batch Processing:** Airflow pipelines summarize hourly traffic data from raw streams.
- **Machine Learning:** An XGBoost regression model predicts future traffic volumes using historical data.
- **Web Application:** A Flask-based dashboard visualizes real-time and predicted traffic on side-by-side interactive maps.

---

## What Works

- **Real-time Data Streaming:** Kafka setup and ingestion of synthetic or real traffic streams.
- **Airflow DAG:** Scheduled batch jobs run to summarize traffic hourly and load data into Snowflake.
- **ML Model Training:** The traffic prediction model is trained using a generated dataset.
- **Flask UI:** Displays two maps side-by-side using Folium — one for real-time traffic and one for predicted traffic.
- **Basic Snowflake Integration:** Data reads and writes are operational for most parts.

---

## Known Issues & Limitations

- **Batch Processing (Airflow) Issues:**  
  The batch summarization DAG intermittently fails or does not process data correctly due to schema mismatches or data availability issues in Snowflake tables. The `HOURLY_SUMMARY` table sometimes lacks timely updates.

- **Training Data:**  
  The machine learning model was trained primarily on synthetic or incomplete data due to batch processing gaps, limiting prediction accuracy.

- **Final Map Visualization:**  
  Although the Flask app runs without errors, the predicted traffic map does not render as expected on the UI. This is likely due to data format mismatches or rendering issues in Folium integration.

- **Snowflake SQL Errors:**  
  Occasional SQL compilation errors related to invalid column identifiers (`LATITUDE`, `TIMESTAMP`, etc.) suggest schema discrepancies between Snowflake tables and the queries in the codebase.

- **Port Conflicts & Environment:**  
  Docker containers for Kafka, Airflow, and web app sometimes face port conflicts or dependency issues affecting startup.

---

## Setup & Usage Instructions

1. **Clone the repository**

   ```bash
   git clone https://github.com/<username>/hamburg-traffic-system.git
   cd hamburg-traffic-system
Docker Compose

Run Kafka, Zookeeper, Postgres, Airflow, and other services:

bash
Copy
docker-compose up --build
Airflow

Initialize the Airflow database before first run:

bash
Copy
docker exec -it airflow-airflow-webserver-1 airflow db init
Access Airflow UI at http://localhost:8081

Snowflake Setup

Configure Snowflake credentials in snowflake_config.json.

Ensure database, schema, and tables exist as per the SQL scripts.

Machine Learning Model

Train the model using train_traffic_model.py.

Save the trained model as model.pkl in the webapp directory.

Web Application

Navigate to webapp directory.

Run Flask app:

bash
Copy
python app.py
Access the UI at http://localhost:8081

Next Steps / Recommendations
Fix Batch Processing:
Debug and fix Airflow DAGs to ensure reliable data aggregation in Snowflake.

Use Real Batch Data for Training:
Retrain ML models on real summarized traffic data for better prediction quality.

Debug Map Rendering:
Investigate Folium map generation logic and ensure correct data formatting.

Schema Consistency:
Align Snowflake table schemas with code SQL queries to avoid compilation errors.

Automate Deployment:
Set up CI/CD pipelines for smoother development and deployment cycles.

Contact
For questions or collaboration, contact Samuel Hanok Anchan.

License
Specify your license here (e.g., MIT License).

pgsql
Copy

If you want, I can also help you format this as a markdown file and provide any additions like badges or installation scripts.




