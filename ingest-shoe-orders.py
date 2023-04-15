# Databricks notebook source
import configparser
config = configparser.ConfigParser()
config.read("config.cfg")

# COMMAND ----------

from pyspark.sql import SparkSession
import pyspark.sql.functions as fn
from pyspark.sql.types import StringType


confluentApiKey= config.get("SHOES-STORE","confluentApiKey")
confluentSecret= config.get("SHOES-STORE","confluentSecret")
confluentTopicName =  config.get("SHOES-STORE","TopicName_shoe_orders")
Bootstrap_server = config.get("SHOES-STORE","bootstrap_server")

tablename = "shoe_orders"


spark = SparkSession.builder \
    .appName(f"KafkaStream-{tablename}") \
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

df = spark.table(f"{tablename}")

display(df.printSchema())

json_schema = df.schema

# COMMAND ----------

from pyspark.sql.functions import from_json, col, current_timestamp, from_unixtime, from_utc_timestamp , to_date,unix_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType, DateType

thai_time_zone = "Asia/Bangkok"

divide_unix_time = lambda x: x /1000 # unix time from sourse this *1000

# Parse the clickstream data
parsed_df = clickstreamTestDf \
     .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
     .select(from_json(col("value"), json_schema).alias("data")) \
     .select("data.*") \
     .withColumnRenamed("ts", "ts_unix") \
     .withColumn("ts_unix", col('ts_unix').cast("double")) \
     .withColumn("ts", from_utc_timestamp(from_unixtime(divide_unix_time(col('ts_unix')), 'yyyy-MM-dd HH:mm:ss'), thai_time_zone)) \
     .dropDuplicates(["order_id"]) \
    .withColumn("arrive_dt_utc", from_unixtime(current_timestamp().cast("double"))) \
     .withColumn("arrive_dt", from_utc_timestamp(col("arrive_dt_utc"), thai_time_zone)) \
     .drop("arrive_dt_utc","ts_unix") \
     .createOrReplaceTempView(f'stream_data_{tablename}')

    #  .withColumn("arrive_dt_utc", from_unixtime(current_timestamp().cast("double"))) \
    #  .withColumn("arrive_dt", from_utc_timestamp(col("arrive_dt_utc"), thai_time_zone)) \
     #  .withColumn('ts', from_unixtime(divide_unix_time(col('ts_unix').cast("double")), 'yyyy-MM-dd HH:mm:ss') ) \
     #.drop("arrive_dt_utc","ts_unix") \

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from stream_data_shoe_orders

# COMMAND ----------

#.createOrReplaceTempView("checking_order")
# convert_time
#      .withColumn("ts", col("ts").cast("long")) \
#      .withColumn("ts_utc", from_unixtime("ts")) \
#      .withColumn("ts", from_utc_timestamp(col("ts_utc"), thai_time_zone)) \
#      .withColumn("arrive_dt_utc", from_unixtime(current_timestamp().cast("long"))) \
#      .withColumn("arrive_dt", from_utc_timestamp(col("arrive_dt_utc"), thai_time_zone)) \
#      .drop("ts_utc", "arrive_dt_utc") 

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Store to bronze table

# COMMAND ----------

#%fs rm -r /mnt/demo/checkpoints

# COMMAND ----------

#%fs rm -r /mnt/datalake/

# COMMAND ----------

#%fs ls /mnt/datalake/

# COMMAND ----------

# write to extenal

# COMMAND ----------

# MAGIC 
# MAGIC %python
# MAGIC 
# MAGIC #get sink table columns names
# MAGIC columns = spark.catalog.listColumns(f'{tablename}')
# MAGIC schema_dict = {}
# MAGIC for col in columns:
# MAGIC     schema_dict[col.name] = col.dataType
# MAGIC 
# MAGIC string_log = ','
# MAGIC arr_of_columns_name = []
# MAGIC arr_of_columns_name_groupby = []
# MAGIC for k,v in schema_dict.items():
# MAGIC     if k != 'arrive_dt' and k != 'ts':
# MAGIC         arr_of_columns_name.append(f's.{k}')  
# MAGIC 
# MAGIC columns_name_statement = string_log.join(arr_of_columns_name)
# MAGIC 
# MAGIC 
# MAGIC # Define a query to aggregate the streaming data
# MAGIC query = f'''
# MAGIC CREATE OR REPLACE TEMP VIEW stream_data_{tablename}_view AS (
# MAGIC   SELECT {columns_name_statement},max(ts) as ts , max(arrive_dt) as arrive_dt
# MAGIC   FROM stream_data_{tablename} s
# MAGIC   GROUP BY {columns_name_statement}
# MAGIC   )
# MAGIC 
# MAGIC '''
# MAGIC spark.sql(query)

# COMMAND ----------

# %sql 
# SELECT * from stream_data_shoe_orders_view

# COMMAND ----------

(spark.table(f"stream_data_{tablename}_view")
    .writeStream
    .format("delta")
    .outputMode("complete") # rewrite each time keep in mind that upstreaming data pipe is only append logic to stream table so it need to rewrite
    .option("checkpointLocation", f"dbfs:/mnt/demo/checkpoints/{tablename}_table")
    .table(tablename))

        #.trigger(once=True) 

# COMMAND ----------

# MAGIC %md
# MAGIC The main difference between .trigger(availableNow=True) and .trigger(once=True) is how the stream is started and how it processes data.
# MAGIC 
# MAGIC .trigger(availableNow=True) starts the stream processing as soon as possible, without waiting for new data to arrive. It processes all the available data in the input source and then stops. This is useful in scenarios where you want to process all the available data in one go, such as in a batch job.
# MAGIC 
# MAGIC .trigger(once=True) starts the stream processing once and then continues processing any new data as it arrives, without stopping. This is useful in scenarios where you want to process data in real-time or near real-time as it becomes available, such as in a streaming job.
# MAGIC 
# MAGIC In summary, .trigger(availableNow=True) is suitable for batch processing, where you want to process all the available data at once, while .trigger(once=True) is suitable for streaming processing, where you want to process data in real-time or near real-time as it becomes available.

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT order_id from stream_data_shoe_orders_view
# MAGIC minus 
# MAGIC SELECT order_id from shoe_orders

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from stream_data_shoe_orders_view where order_id =  969578

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from shoe_orders  where order_id =  969578

# COMMAND ----------

# (spark.table(f"stream_data_{tablename}_view")
#     .writeStream
#     .format("delta")
#     .outputMode("append") # rewrite each time keep in mind that upstreaming data pipe is only append logic to stream table so it need to rewrite
#     .option("checkpointLocation", f"dbfs:/mnt/demo/checkpoints/{tablename}_realtime_table")
#     .trigger(availableNow=True) # batch jobs
#     .table(tablename+'_realtime'))

# COMMAND ----------

# %python
# for s in spark.streams.active:
#   print("Stopping stream: " + s.id)
#   s.stop()
#   s.awaitTermination()

# COMMAND ----------


