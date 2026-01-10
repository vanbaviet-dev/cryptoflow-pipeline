import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import to_date, hour, col
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import from_unixtime
from datetime import datetime, timedelta
import boto3

print("========== Starting ETL Job ==========")
## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

print("========== Reading watermark ==========")
watermark_path = "s3://coin-prices-bucket/metadata/silver_watermark.json"
try:
    wm_df = spark.read.json(watermark_path)
    last_open_time = wm_df.collect()[0]["last_open_time"]
except:
    last_open_time = 0
print(f"========== Last open_time watermark: {last_open_time} ==========")

print("========== Fetching symbols from SSM Parameter Store ==========")
ssm = boto3.client("ssm")
symbols = ssm.get_parameter(Name="/coin/binance/symbols", WithDecryption=False)[
        "Parameter"
    ]["Value"].split(",")
print(f"========== Retrieved symbols: {symbols} ==========")

print("========== Start building paths ==========")
root_path = "s3://coin-prices-bucket/bronze/exchange=binance/"
base_paths=[]
for symbol in symbols:
    symbol = symbol.strip()
    path = f"{root_path}symbol={symbol}/"
    base_paths.append(path)

paths=[]
last_date = datetime.utcfromtimestamp(last_open_time / 1000).date()
print(f"Filtering data after: {last_date} UTC")
for path in base_paths:
    start_date = last_date
    end_date = datetime.utcnow().date() 
    while start_date <= end_date:
        year = start_date.year
        month = start_date.month
        day = start_date.day
        partition_path = f"{path}event_date={year:04d}-{month:02d}-{day:02d}/"
        paths.append(partition_path)
        print(f"Added partition path: {partition_path}")
        start_date += timedelta(days=1)
print("========== Completed building paths ==========")

print("========== Reading data from S3 ==========")
datasource = glueContext.create_dynamic_frame.from_options(
    connection_type = "s3",
    connection_options = {
        "paths": paths, 
        "recurse": True
    },
    format = "json",
    transformation_ctx = "datasource"
)
print(f"Datasource completed with {datasource.count()} records")

print("========== Data type mapping and filtering ==========")
mapped_df = datasource.resolveChoice(specs = [
    ("open", "cast:double"),
    ("high", "cast:double"),
    ("low", "cast:double"),
    ("close", "cast:double"),
    ("volume", "cast:double"),
    ("open_time", "cast:long"),
    ("close_time", "cast:long")
])
df = mapped_df.toDF()
df = df.filter(col("open_time") > last_open_time)
df = (
    df
    .withColumn("event_ts", from_unixtime(col("open_time") / 1000))
    .withColumn("event_date", to_date(col("event_ts")))
    .withColumn("hour", hour(col("event_ts")))
)
df.printschema()
mapped_df = DynamicFrame.fromDF(df, glueContext, "mapped_df")


print(f"mapped_df completed {mapped_df.count()} records")

print("========== Writing data to Silver layer in Parquet format ==========")
output_path = "s3://coin-prices-bucket/silver/"
glueContext.write_dynamic_frame.from_options(
    frame = mapped_df,
    connection_type = "s3",
    connection_options = {
            "path": output_path,
        "partitionKeys": ["exchange", "symbol", "event_date", "hour"] 
    },
    format = "parquet",
    transformation_ctx = "datasink"
)
print("========== Data written to Silver layer ==========")
print("========== Updating watermark ==========")
new_max_open_time = df.agg({"open_time": "max"}).collect()[0][0]
if new_max_open_time:
    spark.createDataFrame(
        [(new_max_open_time,)],
        ["last_open_time"]
    ).write.mode("overwrite").json(watermark_path)
print(f"Updated watermark to last_open_time: {new_max_open_time}")
print("========== ETL Job Completed ==========")
job.commit()