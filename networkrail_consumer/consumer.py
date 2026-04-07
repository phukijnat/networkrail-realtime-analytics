import configparser
import json
from collections import defaultdict
from datetime import datetime, timezone
import os
from time import time
from uuid import uuid4

from google.cloud import storage
from google.oauth2 import service_account
from kafka import KafkaConsumer 




GCP_PROJECT_ID = "dataengineer-bootcamp"
BUCKET_NAME = "deb-bootcamp-37"
BUSINESS_DOMAIN = "networkrail"
DESTINATION_FOLDER = f"{BUSINESS_DOMAIN}/raw"
KEYFILE_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_FOR_GCS")
TOPIC = "networkrail-train-movements"
CONSUMER_GROUP = "phukijnat-networkrail-UTC"

BATCH_SIZE = 1000         # flush เมื่อครบ 1000 records
BATCH_INTERVAL_SECS = 60  # flush ทุก 60 วินาที 

consumer = KafkaConsumer(
    TOPIC,
    # bootstrap_servers=confluent_bootstrap_servers,
    # sasl_mechanism="PLAIN",
    # security_protocol="SASL_SSL",
    # sasl_plain_username=confluent_key,
    # sasl_plain_password=confluent_secret,
    bootstrap_servers='localhost:29092',
    group_id=CONSUMER_GROUP,
    auto_offset_reset="earliest",
)


def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    with open(KEYFILE_PATH, "r") as key_file:
        service_account_info = json.load(key_file)
    credentials = service_account.Credentials.from_service_account_info(service_account_info)

    storage_client = storage.Client(
        project=GCP_PROJECT_ID,
        credentials=credentials,
    )
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)


def get_event_partitions(record: dict) -> tuple[str, str]:
    event_ts = record.get("actual_timestamp") or record.get("planned_timestamp")

    if isinstance(event_ts, str) and event_ts and event_ts != "None":
        try:
            event_dt = datetime.fromisoformat(event_ts.replace("Z", "+00:00"))
            if event_dt.tzinfo is None:
                event_dt = event_dt.replace(tzinfo=timezone.utc)
            else:
                event_dt = event_dt.astimezone(timezone.utc)
            return event_dt.strftime("dt=%Y-%m-%d"), event_dt.strftime("hour=%H")
        except ValueError:
            pass

    now = datetime.now(timezone.utc)
    return now.strftime("dt=%Y-%m-%d"), now.strftime("hour=%H")

def flush_batch(batch: list[dict]):
    if not batch:
        return

    os.makedirs("data", exist_ok=True)

    partitioned_records: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for record in batch:
        dt_partition, hour_partition = get_event_partitions(record)
        partitioned_records[(dt_partition, hour_partition)].append(record)

    for (dt_partition, hour_partition), records in partitioned_records.items():
        timestamp_ms = int(time() * 1000)
        file_name = f"batch-{timestamp_ms}-{uuid4().hex[:8]}.json"
        source_file_name = f"data/{file_name}"
        destination_blob_name = f"{DESTINATION_FOLDER}/{dt_partition}/{hour_partition}/{file_name}"

        with open(source_file_name, "w") as file_handle:
            for record in records:
                file_handle.write(json.dumps(record) + "\n")  # NDJSON: 1 record ต่อบรรทัด

        upload_to_gcs(
            bucket_name=BUCKET_NAME,
            source_file_name=source_file_name,
            destination_blob_name=destination_blob_name,
        )
        print(f"Uploaded {len(records)} records -> {destination_blob_name}")
        os.remove(source_file_name)


batch = []
last_flush = time()

try:
    for message in consumer:
        try:
            data = json.loads(message.value.decode("utf-8"))
            batch.append(data)

            elapsed = time() - last_flush
            if len(batch) >= BATCH_SIZE or elapsed >= BATCH_INTERVAL_SECS:
                flush_batch(batch)
                batch = []
                last_flush = time()

        except json.decoder.JSONDecodeError:
            pass

except KeyboardInterrupt:
    flush_batch(batch)  
finally:
    consumer.close()
