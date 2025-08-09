import sys
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

# ====== Read Raw JSON from S3 ======
raw_path = f"s3://{source_bucket}/raw_data/events/"
df_raw = spark.read.json(raw_path, multiLine=True)

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
