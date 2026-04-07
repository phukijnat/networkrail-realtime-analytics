import logging

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils import timezone


BUSINESS_DOMAIN = "networkrail"
DATA = "movements"
LOCATION = "asia-southeast1"
GCP_PROJECT_ID = "dataengineer-bootcamp"
GCS_BUCKET = "deb-bootcamp-37"
BIGQUERY_DATASET = "networkrail_data"


def _check_processed_files(data_interval_start, **context):
    ds = data_interval_start.to_date_string()
    hour = data_interval_start.strftime("%H")
    prefix = f"{BUSINESS_DOMAIN}/processed/dt={ds}/hour={hour}/"

    hook = GCSHook(gcp_conn_id="load_data_to_gcs")
    blobs = hook.list(GCS_BUCKET, prefix=prefix, max_results=1)

    if not blobs:
        logging.info("No processed files found at %s, skipping downstream.", prefix)
    return bool(blobs)


def _load_data_from_gcs_to_bigquery(data_interval_start, **context):
    ds = data_interval_start.to_date_string()
    hour = data_interval_start.strftime("%H")

    hook = BigQueryHook(gcp_conn_id="load_data_to_bigquery", location=LOCATION)
    hook.create_empty_dataset(
        project_id=GCP_PROJECT_ID,
        dataset_id=BIGQUERY_DATASET,
        location=LOCATION,
        exists_ok=True,
    )

    bucket_name = GCS_BUCKET
    destination_blob_name = f"{BUSINESS_DOMAIN}/processed/dt={ds}/hour={hour}/*.parquet"
    source_uri = f"gs://{bucket_name}/{destination_blob_name}"

    partition_id = f"{ds.replace('-', '')}{hour}"
    job_config = {
        "load": {
            "destinationTable": {
                "projectId": GCP_PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": f"{DATA}${partition_id}",
            },
            "timePartitioning": {
                "type": "HOUR",
                "field": "actual_timestamp",
            },
            "sourceFormat": "PARQUET",
            "writeDisposition": "WRITE_TRUNCATE",
            "sourceUris": [source_uri],
            "autodetect": True,
        }
    }

    job = hook.insert_job(
        configuration=job_config,
        project_id=GCP_PROJECT_ID,
        location=LOCATION,
    )
    job.result()
    job_stats = job._properties.get("statistics", {}).get("load", {})
    logging.info("Loaded %s rows from %s files | source: %s | destination: %s.%s.%s",
                job_stats.get("outputRows", "?"),
                job_stats.get("inputFiles", "?"),
                source_uri, GCP_PROJECT_ID, BIGQUERY_DATASET, DATA)

default_args = {
    "owner": "phukijnat",
    "start_date": timezone.datetime(2024, 8, 25),
}
with DAG(
    dag_id="networkrail_movements",
    default_args=default_args,
    schedule="@hourly",  # Set the schedule here
    catchup=False,
    tags=["pipeline", "networkrail"],
    max_active_runs=3,
):

    # Start
    start = EmptyOperator(task_id="start")

    # Transform data in data lake using Spark
    transform_data = SparkSubmitOperator(
        task_id="transform_data",
        application="/opt/spark/pyspark/transform.py",
        conn_id="my_spark",
        application_args=[
            "--process-date",
            "{{ ds }}",
            "--process-hour",
            "{{ data_interval_start.strftime('%H') }}",
        ],
        )

    # Check if processed files exist in GCS
    check_processed_files = ShortCircuitOperator(
        task_id="check_processed_files",
        python_callable=_check_processed_files,
    )

    # Load data from GCS to BigQuery
    load_data_from_gcs_to_bigquery = PythonOperator(
        task_id="load_data_from_gcs_to_bigquery",
        python_callable=_load_data_from_gcs_to_bigquery,)

    # End
    end = EmptyOperator(task_id="end", trigger_rule="one_success")

    # Task dependencies
    start >> transform_data >> check_processed_files >> load_data_from_gcs_to_bigquery >> end