# src/glue/etl_job.py (upload to s3://bucket/scripts/)
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import functions as F

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Read from catalog:
df = glueContext.create_dynamic_frame.from_catalog(
    database='nyc_taxi_db', table_name='raw'
).toDF()

# Clean + enrich:
df = df.filter(
    (F.col('fare_amount') > 0) &
    (F.col('trip_distance') > 0) &
    (F.col('tpep_pickup_datetime') < F.col('tpep_dropoff_datetime'))
)
df = df.withColumn('trip_duration_mins',
    (F.unix_timestamp('tpep_dropoff_datetime') -
     F.unix_timestamp('tpep_pickup_datetime')) / 60
)
df = df.withColumn('fare_per_mile',
    F.when(F.col('trip_distance') > 0,
    F.col('fare_amount') / F.col('trip_distance'))
)
df = df.withColumn('pickup_date', F.to_date('tpep_pickup_datetime'))
df = df.withColumn('pickup_hour', F.hour('tpep_pickup_datetime'))

# Write partitioned Parquet:
df.write.mode('overwrite').partitionBy('pickup_date').parquet(
    's3://nyc-taxi-de-dsalina/curated/trips/'
)