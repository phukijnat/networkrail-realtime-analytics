import configparser
import json
from datetime import datetime
import os
from time import time

from google.cloud import storage
from google.oauth2 import service_account
from kafka import KafkaConsumer


parser = configparser.ConfigParser()
parser.read("confluent.conf")

confluent_bootstrap_servers = parser.get("config", "confluent_bootstrap_servers")
confluent_key = parser.get("config", "confluent_key")
confluent_secret = parser.get("config", "confluent_secret")

GCP_PROJECT_ID = "dataengineer-bootcamp"
BUCKET_NAME = "deb-bootcamp-37"
BUSINESS_DOMAIN = "networkrail"
DESTINATION_FOLDER = f"{BUSINESS_DOMAIN}/raw"
KEYFILE_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
TOPIC = "networkrail-train-movements"
CONSUMER_GROUP = "phukijnat-networkrail"

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
    service_account_info = json.load(open(KEYFILE_PATH))
    credentials = service_account.Credentials.from_service_account_info(service_account_info)

    storage_client = storage.Client(
        project=GCP_PROJECT_ID,
        credentials=credentials,
    )
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)


def flush_batch(batch: list[dict]):
    if not batch:
        return

    now = datetime.now()
    dt_partition = now.strftime("dt=%Y-%m-%d")
    hour_partition = now.strftime("hour=%H")
    timestamp = int(now.timestamp())

    file_name = f"batch-{timestamp}.json"
    source_file_name = f"data/{file_name}"
    destination_blob_name = f"{DESTINATION_FOLDER}/{dt_partition}/{hour_partition}/{file_name}"

    os.makedirs("data", exist_ok=True)
    with open(source_file_name, "w") as f:
        for record in batch:
            f.write(json.dumps(record) + "\n")  # NDJSON: 1 record ต่อบรรทัด

    upload_to_gcs(
        bucket_name=BUCKET_NAME,
        source_file_name=source_file_name,
        destination_blob_name=destination_blob_name,
    )
    print(f"Uploaded {len(batch)} records -> {destination_blob_name}")


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
