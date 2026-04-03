import logging

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils import timezone


BUSINESS_DOMAIN = "networkrail_data"
DATA = "movements"
LOCATION = "asia-southeast1"
GCP_PROJECT_ID = "dataengineer-bootcamp"
GCS_BUCKET = "deb-bootcamp-37"
BIGQUERY_DATASET = "networkrail"


def _load_data_from_gcs_to_bigquery(data_interval_start, **context):
    ds = data_interval_start.to_date_string()
    hour = data_interval_start.strftime("%H")

    hook = BigQueryHook(gcp_conn_id="load_data_to_gcs", location=LOCATION)

    bucket_name = GCS_BUCKET
    destination_blob_name = f"{BUSINESS_DOMAIN}/processed/dt={ds}/hour={hour}/*/*.parquet"
    source_uri = f"gs://{bucket_name}/{destination_blob_name}"

    configuration = {
        "load": {
            "sourceUris": [source_uri],
            "destinationTable": {
                "projectId": GCP_PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": DATA,
            },
            "sourceFormat": "PARQUET",
            "writeDisposition": "WRITE_APPEND",
            "autodetect": True,
        }
    }

    job = hook.insert_job(
        configuration=configuration,
        project_id=GCP_PROJECT_ID,
        location=LOCATION,
    )
    job.result()
    logging.info("Loaded parquet files from %s into %s.%s.%s", source_uri, GCP_PROJECT_ID, BIGQUERY_DATASET, DATA)

default_args = {
    "owner": "Skooldio",
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

    # Load data from GCS to BigQuery
    load_data_from_gcs_to_bigquery = PythonOperator(
        task_id="load_data_from_gcs_to_bigquery",
        python_callable=_load_data_from_gcs_to_bigquery,)

    # End
    end = EmptyOperator(task_id="end", trigger_rule="one_success")

    # Task dependencies
    start >> transform_data >> load_data_from_gcs_to_bigquery >> end