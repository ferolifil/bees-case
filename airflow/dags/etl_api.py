from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime
import requests
import json
import boto3
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import timedelta

# Variables
API_URL = Variable.get("api_url")
MINIO_ENDPOINT = Variable.get("minio_server")
MINIO_ACCESS_KEY = Variable.get("minio_login")
MINIO_SECRET_KEY = Variable.get("minio_password")
BUCKET_NAME = Variable.get("bronze_path")
FILE_NAME = Variable.get("raw_file_name")
EMAIL = Variable.get("email")
EXPECTED_COLUMNS = {"brewery_type","state"}

def validate_data(df):
    if df.empty:
        raise ValueError("ERROR: Empty dataframe!")
    
    required_columns = ["brewery_type", "state"]
    for col in required_columns:
        if col not in df.columns:
            raise ValueError(f"ERROR: Mising {col} column!")


def extract_data():
    """Extracts data from the API and saves it locally"""
    url = API_URL
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()

        if not data:
            raise ValueError("Error: The API returned an empty dataset!")

        # The JSON file must have "brewery_type" and "state" cols
        missing_columns = EXPECTED_COLUMNS - set(data[0].keys())
        if missing_columns:
            raise ValueError(f"Error: Missing columns in API response: {missing_columns}")

        with open("/tmp/breweries.json", "w") as f:
            json.dump(data, f)
    else:
        raise Exception(f"Error: API request failed with status code {response.status_code}")

def upload_to_minio():
    """Uploads the JSON file to MinIO"""
    s3_client = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY
    )
    
    with open("/tmp/breweries.json", "rb") as f:
        s3_client.upload_fileobj(f, BUCKET_NAME, FILE_NAME)

# DAG definition
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 2, 16),
    "retries": 3,
    "retry_delay": timedelta(minutes=2),  # Wait 2 minutes to retry
    "email_on_failure": True,  # Send e-mail on failure
    "email": EMAIL,
}

with DAG(
    dag_id="api_etl",
    default_args=default_args,
    schedule_interval=timedelta(minutes=5),
    catchup=False
) as dag:

    task_extract = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data,
        sla=timedelta(minutes=10)
    )

    task_upload = PythonOperator(
        task_id="upload_to_minio",
        python_callable=upload_to_minio,
        sla=timedelta(minutes=10)
    )

    trigger_parquet_etl = TriggerDagRunOperator(
        task_id="trigger_parquet_etl",
        trigger_dag_id="parquet_etl"
    )

    task_extract >> task_upload >> trigger_parquet_etl