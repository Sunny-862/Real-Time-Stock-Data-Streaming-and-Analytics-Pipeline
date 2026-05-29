import json
import time
import boto3
from kafka import KafkaConsumer
from botocore.client import Config  

# The REAL MinIO Connection
s3 = boto3.client(
    "s3",
    endpoint_url="http://127.0.0.1:9002",        # <--- Changed BACK to 9002!
    aws_access_key_id="admin",
    aws_secret_access_key="password123",
    region_name="us-east-1",  
    config=Config(
        signature_version="s3v4",                
        s3={'addressing_style': 'path'}
    )
)

bucket_name = "bronze-transaction"

# Define Consumer
consumer = KafkaConsumer(
    "stock-quotes",
    bootstrap_servers=["localhost:29092"], 
    enable_auto_commit=True,
    auto_offset_reset="latest",
    group_id="bronze-consumer",
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

print("Consumer streaming and saving to MinIO...")

# Main Function
for message in consumer:
    record = message.value
    symbol = record.get("symbol")
    ts = record.get("fetched_at", int(time.time()))
    key = f"{symbol}/{ts}.json"

    s3.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=json.dumps(record).encode('utf-8'), 
        ContentType="application/json"
    )