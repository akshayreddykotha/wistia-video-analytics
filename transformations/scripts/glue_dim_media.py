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
    .appName("DimMediaETL") \
    .getOrCreate()

# ====== Read Raw JSON from S3 ======
raw_path = f"s3://{source_bucket}/raw_data/media/"
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
