from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime
import boto3
import pandas as pd
from io import BytesIO
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import timedelta

# Variables
MINIO_ENDPOINT = Variable.get("minio_server")
MINIO_ACCESS_KEY = Variable.get("minio_login")
MINIO_SECRET_KEY = Variable.get("minio_password")
BRONZE_BUCKET_NAME = Variable.get("bronze_path")
SILVER_BUCKET_NAME = Variable.get("silver_path")
RAW_FILE_NAME = Variable.get("raw_file_name")
EMAIL = Variable.get("email")

def read_json_from_minio():
    """Reads JSON file from MinIO and converts it to a Pandas DataFrame"""
    s3_client = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY
    )
    
    response = s3_client.get_object(Bucket=BRONZE_BUCKET_NAME, Key=RAW_FILE_NAME)
    data = response["Body"].read().decode("utf-8")
    
    df = pd.read_json(BytesIO(data.encode()))
    
    return df

def clean_silver_bucket():
    """Deletes all files in the silver bucket before writing new data"""
    s3_client = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )

    response = s3_client.list_objects_v2(Bucket=SILVER_BUCKET_NAME)

    if "Contents" in response:  # Verifica se há arquivos no bucket
        for obj in response["Contents"]:
            s3_client.delete_object(Bucket=SILVER_BUCKET_NAME, Key=obj["Key"])
            print(f"Deleted {obj['Key']} from {SILVER_BUCKET_NAME}")

def transform_and_save_parquet():
    """Transforms data and saves it as a Parquet file partitioned by 'state'"""
    df = read_json_from_minio()

    s3_client = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY
    )

    for state, state_df in df.groupby("state"):
        buffer = BytesIO()
        state_df.to_parquet(buffer, engine="pyarrow", index=False)
        # Move cursor
        buffer.seek(0)

        # Upload partitioned Parquet file
        partitioned_path = f"state={state}/breweries.parquet"
        s3_client.upload_fileobj(buffer, SILVER_BUCKET_NAME, partitioned_path)

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
    dag_id="parquet_etl",
    default_args=default_args,
    catchup=False
) as dag:

    clean_silver_task = PythonOperator(
        task_id="clean_silver_bucket",
        python_callable=clean_silver_bucket,
        sla=timedelta(minutes=10)
    )

    task_transform = PythonOperator(
        task_id="transform_and_save_parquet",
        python_callable=transform_and_save_parquet,
        sla=timedelta(minutes=10)
    )

    trigger_agg_etl = TriggerDagRunOperator(
        task_id="trigger_agg_etl",
        trigger_dag_id="agg_etl"
    )

    clean_silver_task >> task_transform >> trigger_agg_etl