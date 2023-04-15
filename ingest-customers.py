# Databricks notebook source
import configparser
config = configparser.ConfigParser()
config.read("config.cfg")

# COMMAND ----------

# MAGIC %fs rm -r dbfs:/mnt/demo/checkpoints/customers_raw

# COMMAND ----------

# MAGIC %fs rm -r /mnt/datalake/customers_data

# COMMAND ----------

from pyspark.sql import SparkSession
import pyspark.sql.functions as fn
from pyspark.sql.types import StringType

binary_to_string = fn.udf(lambda x: str(int.from_bytes(x, byteorder='big')), StringType())

confluentApiKey= config.get("SHOES-STORE","confluentApiKey")
confluentSecret= config.get("SHOES-STORE","confluentSecret")
confluentTopicName =  config.get("SHOES-STORE","TopicName_customers")
Bootstrap_server = config.get("SHOES-STORE","bootstrap_server")
tablename = "customers"


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

from pyspark.sql.functions import from_json, col, current_timestamp, from_unixtime, from_utc_timestamp , to_date
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType, DateType

thai_time_zone = "Asia/Bangkok"

parsed_df = clickstreamTestDf \
     .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
     .select(from_json(col("value"), json_schema).alias("data")) \
     .select("data.*") \
     .withColumn("arrive_dt", from_unixtime(current_timestamp().cast("long")).cast('timestamp')) \
     .createOrReplaceTempView(f'stream_data_{tablename}')
        
# .withColumn("arrive_dt", from_utc_timestamp(col("arrive_dt_utc"), thai_time_zone)) \
# .drop( "arrive_dt_utc")\
# .dropDuplicates(["id"])

#.table("shoes")
#     .select([col(column_names[i]).cast(data_types[i]).alias(column_names[i]) for i in range(len(column_names))])\
#.drop("ts_utc", "arrive_dt_utc") \     
#.withColumn("arrive_dt_utc", from_unixtime(current_timestamp().cast("long"))) \
#.withColumn("arrive_dt", from_utc_timestamp(col("arrive_dt_utc"), thai_time_zone)) \
#.withColumn("arrive_dt_utc", from_unixtime(current_timestamp().cast("long"))) \
#.withColumn("arrive_dt", to_date(from_utc_timestamp(col("arrive_dt_utc"), thai_time_zone))) \


# COMMAND ----------

# MAGIC %md 
# MAGIC ### Store to bronze table

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
# MAGIC # Define a query to aggregate the streaming data
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
# MAGIC   SELECT {columns_name_statement}, max(arrive_dt) as arrive_dt
# MAGIC   FROM stream_data_{tablename} s
# MAGIC   GROUP BY {columns_name_statement}
# MAGIC   )
# MAGIC 
# MAGIC '''
# MAGIC spark.sql(query)

# COMMAND ----------

(spark.table(f"stream_data_{tablename}_view")
    .writeStream
    .format("delta")
    .outputMode("complete") # rewrite each time keep in mind that upstreaming data pipe is only append logic to stream table so it need to rewrite
    .option("checkpointLocation", f"dbfs:/mnt/demo/checkpoints/{tablename}_table")
    .trigger(once=True) # batch jobs
    .table(tablename))

# COMMAND ----------

# MAGIC %python
# MAGIC for s in spark.streams.active:
# MAGIC   print("Stopping stream: " + s.id)
# MAGIC   s.stop()
# MAGIC   s.awaitTermination()

# COMMAND ----------


