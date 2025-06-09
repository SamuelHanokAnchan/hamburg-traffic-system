from google.cloud import bigquery
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "gcp_key.json"
client = bigquery.Client()

# Create dataset if not exists
dataset_id = "hamburg_traffic"
dataset_ref = client.dataset(dataset_id)
try:
    client.get_dataset(dataset_ref)
except:
    dataset = bigquery.Dataset(dataset_ref)
    client.create_dataset(dataset)
    print(f"Created dataset {dataset_id}")

# Create table
schema = [
    bigquery.SchemaField("timestamp", "TIMESTAMP"),
    bigquery.SchemaField("lat", "FLOAT"),
    bigquery.SchemaField("lon", "FLOAT"),
    bigquery.SchemaField("speed", "INTEGER"),
    bigquery.SchemaField("vehicle_count", "INTEGER"),
]

table_id = f"{dataset_id}.live_stream"
table_ref = dataset_ref.table("live_stream")
try:
    client.get_table(table_ref)
except:
    table = bigquery.Table(table_ref, schema=schema)
    client.create_table(table)
    print(f"Created table {table_id}")
