import sys
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_date, round

# ====== Parse Job Parameters ======
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'TARGET_BUCKET',
    'GLUE_DATABASE'
])

target_bucket = args['TARGET_BUCKET']
glue_db = args['GLUE_DATABASE']

# ====== Create Spark Session ======
spark = SparkSession.builder \
    .appName("FactMediaEngagementETL") \
    .enableHiveSupport() \
    .getOrCreate()

# ====== Read dim_media from Glue Catalog ======
dim_media_df = spark.sql(f"""
    SELECT hashed_id, title, duration
    FROM {glue_db}.dim_media
""")

# ====== Read fact_events from Glue Catalog ======
fact_events_df = spark.sql(f"""
    SELECT media_id, percent_viewed, visitor_key, event_key, received_at
    FROM {glue_db}.fact_events  
""")

# ====== Join and Calculate Watch Time ======
fact_media_engagement_df = fact_events_df.join(
    dim_media_df,
    fact_events_df.media_id == dim_media_df.hashed_id,    
    how="inner"
).withColumn(
    "watch_time_seconds",
    round(col("percent_viewed") / 100 * col("duration"), 2)
).withColumn(
    "load_date", current_date()
).drop(dim_media_df.hashed_id) 

# ====== Write to S3 as Parquet ======
target_path = f"s3://{target_bucket}/processed/fact_media_engagement/"
fact_media_engagement_df.write \
    .mode("overwrite") \
    .partitionBy("load_date") \
    .parquet(target_path)

print(f"Successfully wrote fact_media_engagement data to {target_path}")

spark.stop()
