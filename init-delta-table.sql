-- Databricks notebook source
-- MAGIC %fs ls dbfs:/mnt/demo/

-- COMMAND ----------

-- MAGIC  %fs mkdirs dbfs:/mnt/demo/checkpoints/shoes_raw

-- COMMAND ----------

-- MAGIC  %fs mkdirs dbfs:/mnt/demo/shoes_log

-- COMMAND ----------



-- COMMAND ----------

CREATE database shoe_storex

-- COMMAND ----------

 CREATE OR REPLACE TABLE shoe_storex.shoes (
   id string, brand string, name string, sale_price int, rating double, ts timestamp, arrive_dt timestamp
   )

-- COMMAND ----------

-- MAGIC  %fs mkdirs dbfs:/mnt/demo/checkpoints/customers_raw

-- COMMAND ----------

CREATE OR REPLACE TABLE shoe_storex.customers
 (
   id string,
  first_name string,
  last_name string,
  email string,
  phone string,
  street_address string,
  state string,
  zip_code string,
  country string,
  country_code string, 
  arrive_dt timestamp
  
   )

-- COMMAND ----------

-- MAGIC  %fs mkdirs dbfs:/mnt/demo/checkpoints/shoe_clickstream_raw

-- COMMAND ----------

CREATE OR REPLACE TABLE shoe_storex.shoe_clickstream
 (
   id string,
  first_name string,
  last_name string,
  email string,
  phone string,
  street_address string,
  state string,
  zip_code string,
  country string,
  country_code string,
  ts timestamp,
  arrive_dt timestamp
   )

-- COMMAND ----------


