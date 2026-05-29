import os
import boto3
import snowflake.connector
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from botocore.client import Config  # <-- NEW: Required for Airflow to read MinIO correctly!

MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password123"
BUCKET = "bronze-transaction"
LOCAL_DIR = "/tmp/minio_downloads"

SNOWFLAKE_USER = "stock"
SNOWFLAKE_PASSWORD = "Kadamsunny@135"
SNOWFLAKE_ACCOUNT = "ec74055.ap-southeast-1"
SNOWFLAKE_WAREHOUSE = "COMPUTE_WH"
SNOWFLAKE_DB = "STOCK_MDS"
SNOWFLAKE_SCHEMA = "COMMON"

def get_s3_client():
    # FIX 1: Forcing path-style addressing so Airflow doesn't silently skip files
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4", s3={'addressing_style': 'path'})
    )

def download_from_minio():
    os.makedirs(LOCAL_DIR, exist_ok=True)
    s3 = get_s3_client()
    
    target_folders = ["AAPL", "AMZN", "GOOGL", "MSFT", "TSLA"]
    local_files = []

    for folder in target_folders:
        print(f"Scanning folder: {folder}")
        paginator = s3.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=BUCKET, Prefix=folder):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                safe_filename = key.replace("/", "_")
                local_file = os.path.join(LOCAL_DIR, safe_filename)
                
                s3.download_file(BUCKET, key, local_file)
                print(f"Downloaded {key}")
                local_files.append(local_file)
                
    print(f"Total files downloaded: {len(local_files)}")
    return local_files

def load_to_snowflake(**kwargs):
    local_files = kwargs['ti'].xcom_pull(task_ids='download_minio')
    if not local_files:
        print("No files to load.")
        return

    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DB,
        schema=SNOWFLAKE_SCHEMA
    )
    cur = conn.cursor()

    for f in local_files:
        cur.execute(f"PUT file://{f} @%bronze_stock_quotes_raw")

    # FIX 2: Added PURGE = TRUE so Snowflake destroys files after loading to prevent traffic jams
    cur.execute("""
        COPY INTO bronze_stock_quotes_raw
        FROM @%bronze_stock_quotes_raw
        FILE_FORMAT = (TYPE=JSON)
        PURGE = TRUE
    """)
    
    conn.commit() # FIX 3: Force the database to lock in the transaction

    # Cleanup: Delete from MinIO and Local disk after load
    s3 = get_s3_client()
    
    for f in local_files:
        filename = os.path.basename(f)
        s3_key = filename.replace("_", "/", 1) 
        s3.delete_object(Bucket=BUCKET, Key=s3_key)
        if os.path.exists(f):
            os.remove(f)

    cur.close()
    conn.close()

default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 5, 23),
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    "minio_to_snowflake",
    default_args=default_args,
    schedule_interval="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
) as dag:

    task1 = PythonOperator(task_id="download_minio", python_callable=download_from_minio)
    task2 = PythonOperator(task_id="load_snowflake", python_callable=load_to_snowflake, provide_context=True)

    task1 >> task2