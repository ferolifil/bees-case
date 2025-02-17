from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable 
from datetime import datetime
import boto3
import pandas as pd
from io import BytesIO
from datetime import timedelta

# MinIO settings
MINIO_ENDPOINT = Variable.get("minio_server")
MINIO_ACCESS_KEY = Variable.get("minio_login")
MINIO_SECRET_KEY = Variable.get("minio_password")
SILVER_BUCKET_NAME = Variable.get("silver_path")
GOLD_BUCKET_NAME = Variable.get("gold_path")
OUTPUT_FILE_NAME = Variable.get("agg_file_name")
EMAIL = Variable.get("email")

def read_parquet_files():
    """Reads all Parquet files from MinIO /silver and combines them into a single DataFrame"""
    s3_client = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY
    )

    # List all objects in the silver bucket
    response = s3_client.list_objects_v2(Bucket=SILVER_BUCKET_NAME)
    # Key = file_name
    files = [obj["Key"] for obj in response.get("Contents", []) if obj["Key"].endswith(".parquet")]

    if not files:
        raise ValueError("No Parquet files found in /silver")

    dataframes = []
    for file in files:
        obj = s3_client.get_object(Bucket=SILVER_BUCKET_NAME, Key=file)
        df = pd.read_parquet(BytesIO(obj["Body"].read()))
        dataframes.append(df)

    return pd.concat(dataframes, ignore_index=True)

def aggregate_and_save_csv():
    """Aggregates data by state, counting unique brewery types, and saves as CSV"""
    df = read_parquet_files()

    # Aggregation: count unique brewery types per state
    aggregated_df = df.groupby(['state','brewery_type']).count().reset_index()[["state","brewery_type","id"]].rename(columns={'id': 'count'})

    # Convert DataFrame to CSV
    buffer = BytesIO()
    aggregated_df.to_csv(buffer, index=False)
    # Move cursor
    buffer.seek(0)

    # Upload CSV to MinIO /gold
    s3_client = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY
    )
    s3_client.upload_fileobj(buffer, GOLD_BUCKET_NAME, OUTPUT_FILE_NAME)

# DAG definition
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 2, 15),
    "retries": 3,
    "retry_delay": timedelta(minutes=2),  # Wait 2 minutes to retry
    "email_on_failure": True,  # Send e-mail on failure
    "email": EMAIL,
}

with DAG(
    dag_id="agg_etl",
    default_args=default_args,
    catchup=False
) as dag:

    task_aggregate = PythonOperator(
        task_id="aggregate_and_save_csv",
        python_callable=aggregate_and_save_csv,
        sla=timedelta(minutes=10)
    )

    task_aggregate
