from kafka import KafkaConsumer
import boto3
import json
import os
import time

consumer = KafkaConsumer(
    'user_search',
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    group_id='user-search-group'
)

s3 = boto3.client('s3')
bucket_name = os.getenv("AWS_BUCKET_NAME", "bdm-project-upc")

if __name__ == "__main__":

    while True:
        for message in consumer:
            event = message.value
            print(f"[Consumer] Event received: {event}")

            user_id = event["user_id"]
            register_date = event["search_date"].split("T")[0]
            hour_date = event["search_date"].split("T")[1]
            s3_key = f"raw/streaming/event_type=search/date={register_date}/user_{user_id}_{hour_date}.json"

            s3.put_object(
                Bucket=bucket_name,
                Key=s3_key,
                Body=json.dumps(event),
                ContentType='application/json'
            )
            print(f"[S3] Saved in {s3_key}")
            time.sleep(20)