-- Databricks notebook source
-- MAGIC %md
-- MAGIC #SILVER LAYLER

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### **Click stream history**

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC CREATE OR REPLACE TABLE cust_click_stream_history AS (
-- MAGIC   SELECT cs.*, DATEDIFF(hour, cs.ts, current_timestamp()) as hours_since_timestamp,
-- MAGIC         DATEDIFF(day, cs.ts, current_timestamp()) as days_since_timestamp,
-- MAGIC         DATEDIFF(month, cs.ts, current_timestamp()) as month_since_timestamp
-- MAGIC   FROM shoe_clickstream cs
-- MAGIC )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Customer intention history

-- COMMAND ----------

CREATE OR REPLACE TABLE cust_intent_history AS (
with 
  brand_click_info as (  
            select c.user_id, s.id as shoes_id, s.brand, avg(view_time) as avg_view_time, sum(view_time) as total_view_time, sum(1) as click_count
            from cust_click_stream_history c 
            join shoes s on c.product_id = s.id
            group by c.user_id,s.id,s.brand),
  order_info as (
            
            select so.customer_id,so.product_id as shoes_id, sum(1) as buy_count
            from shoe_orders so 
            join shoes s on so.product_id = s.id
            group by so.customer_id,so.product_id
  )
select b.* , 
(case when oi.buy_count >0 then oi.buy_count else 0 end) buy_count 
from  brand_click_info b 
left join order_info oi on b.user_id = oi.customer_id and b.shoes_id = oi.shoes_id
)
  

-- COMMAND ----------

-- WITH click_info AS ( 
--   SELECT 
--     ch.user_id,
--     ch.brand AS most_interest_brand,
--     ROW_NUMBER() OVER (PARTITION BY ch.user_id ORDER BY buy_count DESC) AS row_num
--   FROM (
--     SELECT 
--       user_id,
--       brand,
--       SUM(buy_count) AS buy_count
--     FROM 
--       cust_intent_history
--     GROUP BY 
--       user_id,
--       brand
--   ) ch
-- ), 
-- top_interest as (
-- SELECT  
--  * 
-- FROM click_info 
-- PIVOT (
--   min(most_interest_brand)
--   FOR row_num in (
--     1 most_interest_brand,2 second_interest_brand
--   )
-- )
-- ORDER BY user_id DESC)
-- select *
--   from top_interest t

-- COMMAND ----------

-- fix the most purchased brand for each user
with cust_latest_click as (
      select 
            c.user_id, 
             c.hours_since_timestamp as latest_interact,
            ROW_NUMBER() OVER (partition by c.user_id ORDER BY c.ts) AS row_num -- most click =1 
      from cust_click_stream_history c  
  ), 
  enrich_latest_click as (
      select 
      user_id,
      latest_interact as latest_interact_hour
      from cust_latest_click 
      where row_num =1
      ),
  click_info as ( 
      SELECT 
        ch.user_id,
        ch.brand AS most_interest_brand,
        ROW_NUMBER() OVER (PARTITION BY ch.user_id ORDER BY buy_count DESC) AS row_num
      FROM (
        SELECT 
          user_id,
          brand,
          SUM(buy_count) AS buy_count
        FROM 
          cust_intent_history
        GROUP BY 
          user_id,
          brand
      ) ch
        ),
  enrich_click as (  --find to user interest product
        SELECT  
         *
        FROM click_info 
        PIVOT (
          min(most_interest_brand)
          FOR row_num in (
           1 most_interest_brand,2 second_interest_brand
          )
        )
        ORDER BY user_id DESC
               ),
  order_info as (
      select 
      ch.user_id, 
      sum(ch.total_view_time) as total_view_time, 
      sum(ch.click_count) as click_count, 
      sum(ch.buy_count) as total_buy 
      from cust_intent_history ch
      group by ch.user_id
  )
select lc.*, ec.most_interest_brand, ec.second_interest_brand, oi.total_view_time, oi.click_count, oi.total_buy 
from enrich_latest_click lc
join enrich_click ec on lc.user_id = ec.user_id
join order_info oi on lc.user_id = oi.user_id

-- COMMAND ----------

-- label table

-- COMMAND ----------

-- find conversion_rate per click

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### **Transaction history table**

-- COMMAND ----------

-- MAGIC %python
-- MAGIC columns_shoe_orders = spark.catalog.listColumns('shoe_orders')
-- MAGIC columns_customers = spark.catalog.listColumns('customers')
-- MAGIC columns_shoes = spark.catalog.listColumns('shoes')
-- MAGIC 
-- MAGIC columns_shoe_orders_rename = [i.name+' as orders_'+i.name if i.name == 'id' or i.name == 'arrive_dt' or i.name == 'ts' else i.name for i in columns_shoe_orders]
-- MAGIC columns_customers_rename = [i.name+' as customers_'+i.name if i.name == 'id' or i.name == 'arrive_dt' or i.name == 'ts' else i.name for i in columns_customers]
-- MAGIC columns_shoes_rename = [i.name+' as shoes_'+i.name if i.name == 'id' or i.name == 'arrive_dt' or i.name == 'ts' else i.name for i in columns_shoes ]

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # find dup col name
-- MAGIC set(columns_customers_rename).intersection(columns_customers_rename).intersection(columns_shoes_rename)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC columns_shoe_orders_statement = ','.join(['so.'+i for i  in columns_shoe_orders_rename])
-- MAGIC columns_customers_statement = ','.join(['cs.'+i for i in columns_customers_rename])
-- MAGIC columns_shoes_statement = ','.join(['c.'+i for i in columns_shoes_rename])

-- COMMAND ----------

# it uniuqe
select s.id,sum(1) from shoes s group by s.id having sum(1)>1

-- COMMAND ----------

-- MAGIC %python
-- MAGIC qeury_create_transction_orders = f'''
-- MAGIC CREATE OR REPLACE TABLE transction_orders AS (
-- MAGIC   SELECT {columns_shoe_orders_statement}, {columns_customers_statement}, {columns_shoes_statement}, date_format(so.ts, 'yyyyMM') as orders_ts_month
-- MAGIC   FROM shoe_orders so 
-- MAGIC   LEFT JOIN customers cs ON so.customer_id = cs.id
-- MAGIC   LEFT JOIN shoes c ON so.product_id = c.id
-- MAGIC )
-- MAGIC '''
-- MAGIC 
-- MAGIC qeury_create_transction_orders_index = '''
-- MAGIC -- create an index on cust_id column
-- MAGIC CREATE INDEX transction_orders_cust_id_index
-- MAGIC ON transction_orders (customers_id)
-- MAGIC OPTIONS ('delta.autoOptimize.optimizeWrite'='true');
-- MAGIC '''
-- MAGIC 
-- MAGIC spark.sql(qeury_create_transction_orders)
-- MAGIC #spark.sql(qeury_create_transction_orders_index)

-- COMMAND ----------

-- silver filter non perches
-- find converion rate from click steam 
-- top sell product
-- top customer and how long sinc last time 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###GLOD LAYER

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC -- top sell 
-- MAGIC CREATE OR REPLACE view TABLE top_sell_product AS (
-- MAGIC select so.product_id as shoes_id, s.brand, s.sale_price, s.rating, sum(1) as total_sales
-- MAGIC       from shoe_orders so 
-- MAGIC       join shoes s on so.product_id = s.id
-- MAGIC       group by so.product_id, s.brand, s.sale_price, s.rating )

-- COMMAND ----------

 -- lastest_click -- hot intent in this week
CREATE OR REPLACE TEMPORARY VIEW click_within_week_view AS (
  WITH click_within_week AS (
    SELECT c.*
    FROM cust_click_stream_history c 
    WHERE c.days_since_timestamp < 7
  )
  SELECT user_id, product_id, 
         SUM(view_time) AS view_time 
  FROM click_within_week 
  GROUP BY user_id, product_id)

-- COMMAND ----------

-- %python
-- for s in spark.streams.active:
--   print("Stopping stream: " + s.id)
--   s.stop()
--   s.awaitTermination()
