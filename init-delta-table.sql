-- Databricks notebook source
-- MAGIC %fs ls dbfs:/mnt/demo/

-- COMMAND ----------

--drop table shoes

-- COMMAND ----------

--CREATE database shoes

-- COMMAND ----------

 CREATE OR REPLACE TABLE shoes (
   id string, brand string, name string, sale_price int, rating double, arrive_dt timestamp
   )

-- COMMAND ----------

 CREATE OR REPLACE TABLE shoes_test (
   id string, brand string, name string, sale_price int, rating double, arrive_dt timestamp
   )

-- COMMAND ----------

CREATE OR REPLACE TABLE customers
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

CREATE OR REPLACE TABLE shoe_clickstream
 (
  product_id string,
  user_id string,
  view_time int,
  page_url string,
  ip string,
  ts timestamp,
  arrive_dt timestamp
   )

-- COMMAND ----------

CREATE OR REPLACE TABLE shoe_orders
 (
  order_id int,
  product_id string,
  customer_id string,
  ts timestamp,
  arrive_dt timestamp
   )

-- COMMAND ----------


