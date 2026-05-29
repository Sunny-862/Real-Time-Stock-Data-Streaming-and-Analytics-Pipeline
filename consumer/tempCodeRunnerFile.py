import json
import time
import boto3
from kafka import KafkaConsumer
from botocore.client import Config  # <--- This is the magic line that was missing!

# MinIO Connection
s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id="admin",
    aws_secret_access_key="password123",
    config=Config(s3={'addressing_style': 'path'})
)

bucket_name = "bronze-transaction"

# Define Consumer
consumer = KafkaConsumer(
    "stock-quotes",
    bootstrap_servers=["localhost:29092"], # Fixed to localhost
    enable_auto_commit=True,
    auto_offset_reset="earliest",
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
        Body=json.dumps(record),
        ContentType="application/json"
    )