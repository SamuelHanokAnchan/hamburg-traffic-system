from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import snowflake.connector
import json

def summarize_traffic():
    # Load config
    with open("/opt/airflow/snowflake_config.json", "r") as f:
        config = json.load(f)

    # Connect to Snowflake
    conn = snowflake.connector.connect(
        user=config["user"],
        password=config["password"],
        account=config["account"],
        warehouse=config["warehouse"],
        database=config["database"],
        schema=config["schema"]
    )
    cs = conn.cursor()

    # Summarize and upsert hourly data
    try:
        cs.execute("""
            MERGE INTO HOURLY_SUMMARY AS target
            USING (
                SELECT
                    DATE_TRUNC('HOUR', timestamp) AS HOUR,
                    COUNT(*) AS TOTAL_VEHICLES,
                    AVG(speed) AS AVG_SPEED
                FROM LIVE_STREAM
                WHERE timestamp >= DATEADD(HOUR, -1, CURRENT_TIMESTAMP())
                GROUP BY 1
            ) AS source
            ON target.HOUR = source.HOUR
            WHEN MATCHED THEN
              UPDATE SET TOTAL_VEHICLES = source.TOTAL_VEHICLES, AVG_SPEED = source.AVG_SPEED
            WHEN NOT MATCHED THEN
              INSERT (HOUR, TOTAL_VEHICLES, AVG_SPEED)
              VALUES (source.HOUR, source.TOTAL_VEHICLES, source.AVG_SPEED)
        """)
        print("HOURLY_SUMMARY updated successfully.")
    finally:
        cs.close()
        conn.close()

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 6, 8),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='traffic_summary_dag',
    default_args=default_args,
    schedule_interval='@hourly',
    catchup=False,
) as dag:
    summarize_task = PythonOperator(
        task_id='summarize_hourly_traffic',
        python_callable=summarize_traffic
    )
