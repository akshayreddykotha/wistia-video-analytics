import sys
import boto3
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
    .appName("DimMediaETL") \
    .getOrCreate()

# ====== Read Raw JSON from S3 ======
metadata_key = "raw_data/media/s3_keys/latest_media_file.json"  # example key where the pointer is stored

# Initialize boto3 S3 client
s3 = boto3.client("s3")

# Read the metadata file content from S3 (no local temp file)
obj = s3.get_object(Bucket=source_bucket, Key=metadata_key)
content = obj['Body'].read().decode('utf-8')

# Parse JSON to get the latest file path (assuming JSON structure like {"latest_s3_key": "raw_data/media/filename.json"})
metadata = json.loads(content)
latest_file_key = metadata.get("latest_s3_key")
if not latest_file_key:
    raise Exception("latest_s3_key not found in metadata JSON")

# Full S3 path to latest JSON file - Implementing SCD1
latest_file_s3_path = f"s3://{source_bucket}/{latest_file_key}"
df_raw = spark.read.json(raw_path, multiLine=True)

# ====== Flatten & Select Required Columns ======
df_dim_media = df_raw.select(
    col("id").alias("media_id"),
    col("name").alias("title"),
    col("thumbnail.url").alias("url"),  # Example URL
    col("duration").alias("duration"),  # Adjust if your API field differs
    col("created").alias("created_at"),
    col("updated").alias("updated_at"),
    col("hashed_id").alias("hashed_id")
).withColumn("load_date", current_date())

# ====== Write to Parquet in Process Folder ======
target_path = f"s3://{target_bucket}/processed/dim_media/"
df_dim_media.write \
    .mode("append") \
    .partitionBy("load_date") \
    .parquet(target_path)

print(f"Successfully wrote dim_media data to {target_path}")

spark.stop()
