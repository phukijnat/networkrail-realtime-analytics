import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, regexp_extract
from pyspark.sql.types import StructField, StructType, StringType, TimestampType
from pyspark.sql.utils import AnalysisException



BUCKET_NAME = "deb-bootcamp-37"
BUSINESS_DOMAIN = "networkrail"
SOURCE_FOLDER = f"{BUSINESS_DOMAIN}/raw"
DESTINATION_FOLDER = f"{BUSINESS_DOMAIN}/processed"
KEYFILE_PATH = "/opt/spark/pyspark/deb-uploading-files-to-gcs.json"


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--process-date", required=True, help="Date in YYYY-MM-DD")
    parser.add_argument("--process-hour", required=True, help="Hour in HH")
    return parser.parse_args()


args = parse_args()
PROCESS_DATE = args.process_date
PROCESS_HOUR = args.process_hour

spark = SparkSession.builder.appName("networkrail_transform") \
    .config("spark.memory.offHeap.enabled", "true") \
    .config("spark.memory.offHeap.size", "5g") \
    .config("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("google.cloud.auth.service.account.enable", "true") \
    .config("google.cloud.auth.service.account.json.keyfile", KEYFILE_PATH) \
    .getOrCreate()

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set("spark.sql.session.timeZone", "UTC")

struct_schema = StructType([
    StructField("actual_timestamp", TimestampType()),
    StructField("auto_expected", StringType()),
    StructField("correction_ind", StringType()),
    StructField("current_train_id", StringType()),
    StructField("delay_monitoring_point", StringType()),
    StructField("direction_ind", StringType()),
    StructField("division_code", StringType()),
    StructField("event_source", StringType()),
    StructField("event_type", StringType()),
    StructField("gbtt_timestamp", TimestampType()),
    StructField("line_ind", StringType()),
    StructField("loc_stanox", StringType()),
    StructField("next_report_run_time", StringType()),
    StructField("next_report_stanox", StringType()),
    StructField("offroute_ind", StringType()),
    StructField("original_loc_stanox", StringType()),
    StructField("original_loc_timestamp", TimestampType()),
    StructField("planned_event_type", StringType()),
    StructField("planned_timestamp", TimestampType()),
    StructField("platform", StringType()),
    StructField("reporting_stanox", StringType()),
    StructField("route", StringType()),
    StructField("timetable_variation", StringType()),
    StructField("toc_id", StringType()),
    StructField("train_id", StringType()),
    StructField("train_file_address", StringType()),
    StructField("train_service_code", StringType()),
    StructField("train_terminated", StringType()),
    StructField("variation_status", StringType()),
])

GCS_FILE_PATH = f"gs://{BUCKET_NAME}/{SOURCE_FOLDER}/dt={PROCESS_DATE}/hour={PROCESS_HOUR}/*.json"


try:
    df = spark.read.schema(struct_schema).json(GCS_FILE_PATH)
except AnalysisException:
    print(f"Path not found: {GCS_FILE_PATH}, skipping.")
    spark.stop()
    exit(0)

if df.rdd.isEmpty():
    print(f"No data found for {GCS_FILE_PATH}, skipping.")
    spark.stop()
    exit(0)

df = df.withColumn("source_file", input_file_name())
df = df.withColumn("dt", regexp_extract("source_file", r"dt=(\d{4}-\d{2}-\d{2})", 1))
df = df.withColumn("hour", regexp_extract("source_file", r"hour=(\d{2})", 1))

df.show()
df.printSchema()

df.createOrReplaceTempView("networkrail")
result = spark.sql("""
    select
        *

    from networkrail
""")

result = result.drop("source_file")

OUTPUT_PATH = f"gs://{BUCKET_NAME}/{DESTINATION_FOLDER}"
result.write.mode("overwrite").partitionBy("dt", "hour").parquet(OUTPUT_PATH)
