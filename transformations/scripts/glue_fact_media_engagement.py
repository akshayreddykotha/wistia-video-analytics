import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.sql.functions import col, current_date, round
from pyspark.context import SparkContext

# ====== Parse Job Parameters ======
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'TARGET_BUCKET',
    'GLUE_DATABASE'
])

target_bucket = args['TARGET_BUCKET']
glue_db = args['GLUE_DATABASE']

# ====== Create Spark & Glue Context ======
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# ====== Read dim_media from Glue Catalog ======
dim_media_df = glueContext.create_dynamic_frame.from_catalog(
    database=glue_db,
    table_name="dim_media"
).toDF().select("hashed_id", "title", "duration")

# ====== Read fact_events from Glue Catalog ======
fact_events_df = glueContext.create_dynamic_frame.from_catalog(
    database=glue_db,
    table_name="fact_events"
).toDF().select("media_id", "percent_viewed", "visitor_key", "event_key", "received_at")

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