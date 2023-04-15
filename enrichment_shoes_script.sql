-- Databricks notebook source
-- MAGIC %md 
-- MAGIC # Enrichment in Silver layer

-- COMMAND ----------

-- silver
-- * join to Customer click history
-- * join to transaction table 
-- gold
-- * customer feature table -> ML model -> find customer who likely to buy a product -> send ad
-- * top_sell_product -> analytic model -> find Usually the best-selling shoes
-- * click monitor -> find a customer who is most likely to buy a product by using click to view 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC def get_col_name(table_name=[], perfix = [], except_col=[]):
-- MAGIC     table_list = []
-- MAGIC     for count, i in enumerate(table_name):
-- MAGIC         # i is table name
-- MAGIC         alias = perfix[count]
-- MAGIC         columns_name = spark.catalog.listColumns(i)
-- MAGIC 
-- MAGIC         columns_name = [c.name for c in columns_name if c.name not in except_col]
-- MAGIC 
-- MAGIC         columns_rename = [f"{alias}.{c} as {i}_{c}" if c in ["id", "arrive_dt", "ts"] else f"{alias}.{c}" for c in columns_name]
-- MAGIC         print(f"table name {i}")
-- MAGIC         print(columns_rename)
-- MAGIC 
-- MAGIC         for j in columns_rename:
-- MAGIC             table_list.append(j)
-- MAGIC         columns_statement = ", ".join(table_list)
-- MAGIC     return columns_statement

-- COMMAND ----------

-- MAGIC %md
-- MAGIC *  click stream history

-- COMMAND ----------

-- %python
-- columns_statement = get_col_name(table_name = ['shoe_clickstream','shoes'],perfix = ['cs','c'] ,except_col = ['arrive_dt'])
-- print(columns_statement)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC columns_statement = get_col_name(table_name = ['shoe_clickstream','shoes'],perfix = ['cs','c'] ,except_col = ['arrive_dt'])
-- MAGIC print(columns_statement)
-- MAGIC 
-- MAGIC qeury_cust_click_stream_history = f'''CREATE OR REPLACE temp view cust_click_stream_history
-- MAGIC AS (
-- MAGIC   SELECT {columns_statement}, 
-- MAGIC          DATEDIFF(hour, cs.ts, current_timestamp()) as hours_since_timestamp,
-- MAGIC          DATEDIFF(day, cs.ts, current_timestamp()) as days_since_timestamp,
-- MAGIC          DATEDIFF(month, cs.ts, current_timestamp()) as month_since_timestamp
-- MAGIC   from shoe_clickstream cs
-- MAGIC   join shoes s on cs.product_id = s.id 
-- MAGIC )'''
-- MAGIC 
-- MAGIC spark.sql(qeury_cust_click_stream_history )

-- COMMAND ----------

-- MAGIC %md * transaction table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC 
-- MAGIC columns_statement = get_col_name(table_name = ['shoe_orders','customers','shoes'],perfix = ['so','cs','c'] ,except_col = ['arrive_dt'])
-- MAGIC print(columns_statement)
-- MAGIC 
-- MAGIC qeury_create_transction_orders = f'''
-- MAGIC CREATE OR REPLACE TABLE orders_transction AS (
-- MAGIC   SELECT {columns_statement}, date_format(so.ts, 'yyyyMM') as orders_ts_month
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

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC SELECT * from orders_transction

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Gold layer

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC * customer_order_history

-- COMMAND ----------

-- MAGIC %python
-- MAGIC query = '''
-- MAGIC CREATE OR REPLACE VIEW customer_order_history AS (
-- MAGIC with 
-- MAGIC   order_info as (
-- MAGIC             
-- MAGIC             select oh.shoe_orders_ts,oh.customer_id,oh.product_id,oh.brand, oh.name, sum(1) as bought_count
-- MAGIC             from orders_transction oh
-- MAGIC             group by oh.shoe_orders_ts,oh.customer_id,oh.product_id,oh.brand, oh.name
-- MAGIC   )
-- MAGIC select oi.*
-- MAGIC from order_info oi
-- MAGIC 
-- MAGIC )
-- MAGIC '''
-- MAGIC spark.sql(query)

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC SELECT * from customer_order_history

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * top sell product

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC -- top sell 
-- MAGIC CREATE OR REPLACE VIEW top_sell_product AS (
-- MAGIC select shoes_id, brand, name, sale_price, rating, sum(1) as total_sales
-- MAGIC       from orders_transction
-- MAGIC       group by shoes_id, brand, sale_price, rating)

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC SELECT * from top_sell_product

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * customer feature

-- COMMAND ----------

select * from cust_click_stream_history

-- COMMAND ----------

with 
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
          cust_click_stream_history
        GROUP BY 
          user_id,
          brand
      ) ch
        )
select * from click_info

-- COMMAND ----------

-- fix the most purchased brand for each user
with 
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
          cust_click_stream_history
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

-- MAGIC %md
-- MAGIC * click monitor -> find a customer who just clicked on our page within the last 15 minutes

-- COMMAND ----------


CREATE OR REPLACE temp view real_time_click_stream_15_minutes AS (
SELECT * 
from cust_click_stream_history ch 
where ch.ts  >= (current_timestamp() - INTERVAL 15 MINUTES))

-- COMMAND ----------


