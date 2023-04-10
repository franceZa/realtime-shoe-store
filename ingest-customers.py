# Databricks notebook source
from pyspark.sql import SparkSession
import pyspark.sql.functions as fn
from pyspark.sql.types import StringType

binary_to_string = fn.udf(lambda x: str(int.from_bytes(x, byteorder='big')), StringType())

confluentApiKey= '${confluentApiKey}'
confluentSecret= '${confluentSecret}'
confluentTopicName =  '${topicNamecustomers}'
Bootstrap_server = '${bootstrap_server}'
spark = SparkSession.builder \
    .appName("KafkaStream-customer") \
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

# MAGIC %sql
# MAGIC DESCRIBE DETAIL shoe_storex.shoes 

# COMMAND ----------

df = spark.table(f"shoe_storex.customers")
json_schema = df.schema

# COMMAND ----------

from pyspark.sql.functions import from_json, col, current_timestamp,from_unixtime,timezone
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType
thai_time_zone = "Asia/Bangkok"

parsed_df = clickstreamTestDf \
     .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
     .select(from_json(col("value"), json_schema).alias("data")) \
     .select("data.*") \
     .withColumn("arrive_dt", timezone(thai_time_zone,from_unixtime(current_timestamp().cast("long")))) \
     .dropDuplicates(["id"]) \
     .writeStream \
     .format("delta") \
     .option("checkpointLocation", "dbfs:/mnt/demo/checkpoints/customers_raw") \
     .outputMode("append") \
     .table("shoe_storex.customers")


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REFRESH STREAMING LIVE TABLE customers_live
# MAGIC COMMENT ""
# MAGIC AS SELECT * FROM cloud_files( "dbfs:/mnt/demo/checkpoints/customers_raw", "delta",
# MAGIC                              map("cloudFiles.inferColumnTypes", "true"))

# COMMAND ----------


