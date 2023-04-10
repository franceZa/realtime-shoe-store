# Databricks notebook source
from pyspark.sql import SparkSession
import pyspark.sql.functions as fn
from pyspark.sql.types import StringType

binary_to_string = fn.udf(lambda x: str(int.from_bytes(x, byteorder='big')), StringType())

confluentApiKey='${confluentApiKey}'
confluentSecret='${confluentSecret}'
confluentTopicName =  '${topicNameshoe}'
Bootstrap_server = '${bootstrap_server}'

spark = SparkSession.builder \
    .appName("KafkaStream-shoes") \
    .getOrCreate()

clickstreamTestDf = (
  spark
  .readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", Bootstrap_server)
  .option("kafka.security.protocol", "SASL_SSL")
  .option("kafka.sasl.jaas.config",
          "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='{}' password='{}';".format(confluentApiKey, confluentSecret))
  .option("kafka.ssl.endpoint.identification.algorithm", "https")
  .option("kafka.sasl.mechanism", "PLAIN")
  .option("subscribe", confluentTopicName)
  .option("startingOffsets", "earliest")
  .option("failOnDataLoss", "false")
  .load()
)
#.createOrReplaceTempView("shoes_raw_temp")



# COMMAND ----------

# confluentApiKey="${confluentApiKey}"
# confluentApiKey

# COMMAND ----------

df = spark.table(f"shoe_storex.shoes")
json_schema = df.schema
json_schema 

# COMMAND ----------

from pyspark.sql.functions import from_json, col, current_timestamp, from_unixtime, from_utc_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType

thai_time_zone = "Asia/Bangkok"

parsed_df = clickstreamTestDf \
     .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
     .select(from_json(col("value"), json_schema).alias("data")) \
     .select("data.*") \
     .withColumn("ts", col("ts").cast("long")) \
     .withColumn("ts_utc", from_unixtime("ts")) \
     .withColumn("ts", from_utc_timestamp(col("ts_utc"), thai_time_zone)) \
     .withColumn("arrive_dt_utc", from_unixtime(current_timestamp().cast("long"))) \
     .withColumn("arrive_dt", from_utc_timestamp(col("arrive_dt_utc"), thai_time_zone)) \
     .drop("ts_utc", "arrive_dt_utc") \
     .dropDuplicates(["id"]) \
     .writeStream \
     .format("delta") \
     .option("checkpointLocation", "dbfs:/mnt/demo/checkpoints/shoes_raw") \
     .outputMode("append") \
     .table("shoe_storex.shoes")
     


# COMMAND ----------

# %sql 
# CREATE OR REPLACE TEMPORARY LIVE VIEW shoes_raw_view AS (
#     SELECT id, brand, name, sale_price, rating, ts, arrive_dt
#     FROM shoes_raw_temp

# )

# COMMAND ----------

# %sql
# select * from shoes_raw_view

# COMMAND ----------

# (spark.table("shoes_raw_view")
#   .dropDuplicates(["id"])
#   .writeStream
#   .format("delta")
#   .option("checkpointLocation", "dbfs:/mnt/demo/checkpoints/shoes_raw")
#   .outputMode("append")
#   .table("shoe_storex.shoes"))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REFRESH STREAMING LIVE TABLE shoes_live
# MAGIC COMMENT ""
# MAGIC AS SELECT * FROM cloud_files( "dbfs:/mnt/demo/checkpoints/shoes_raw", "delta",
# MAGIC                              map("cloudFiles.inferColumnTypes", "true"))

# COMMAND ----------


