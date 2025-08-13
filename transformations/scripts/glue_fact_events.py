import sys
import boto3
import json
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_date

# ====== Parse Job Parameters ======
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'SOURCE_BUCKET',
    'TARGET_BUCKET'
])

source_bucket = args['SOURCE_BUCKET']
target_bucket = args['TARGET_BUCKET']

# ====== Create Spark Session ======
spark = SparkSession.builder \
    .appName("FactEventsETL") \
    .getOrCreate()

# ====== Read Latest S3 Key from metadata ======
metadata_key = "raw_data/events/s3_keys/latest_event_file.json"
s3 = boto3.client("s3")

try:
    obj = s3.get_object(Bucket=source_bucket, Key=metadata_key)
    content = obj['Body'].read().decode('utf-8')
    metadata = json.loads(content)
    latest_file_key = metadata.get("latest_s3_key")
except s3.exceptions.NoSuchKey:
    latest_file_key = None

if not latest_file_key:
    print("No new event file found. Skipping ETL run.")
    spark.stop()
    print("Moving to next step with skipped Glue job...")


# Full S3 path to the latest JSON file
latest_file_s3_path = f"s3://{source_bucket}/{latest_file_key}"
# ====== Read Raw JSON from latest file ======
df_raw = spark.read.json(latest_file_s3_path, multiLine=True)

# ====== Flatten & Select Required Columns ======
df_fact_events = df_raw.select(
    col("visitor_key").alias("visitor_key"),
    col("event_key").alias("event_key"),
    col("received_at").alias("received_at"),  
    col("percent_viewed").alias("percent_viewed"),  
    col("ip").alias("ip"),
    col("country").alias("country"),
    col("media_id").alias("media_id"),
    col("media_url").alias("media_url")
).withColumn("load_date", current_date())

# ====== Write to Parquet in Process Folder ======
target_path = f"s3://{target_bucket}/processed/fact_events/"
df_fact_events.write \
    .mode("append") \
    .partitionBy("load_date") \
    .parquet(target_path)

print(f"Successfully wrote fact_events data to {target_path}")

spark.stop()
