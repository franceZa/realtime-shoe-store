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
-- MAGIC columns_statement = get_col_name(table_name = ['shoe_clickstream','shoes'],perfix = ['cs','s'] ,except_col = ['arrive_dt'])
-- MAGIC print(columns_statement)
-- MAGIC 
-- MAGIC qeury_cust_click_stream_history = f'''CREATE OR REPLACE table cust_click_stream_history
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
-- MAGIC CREATE OR REPLACE VIEW top_sell_product_all_time AS (
-- MAGIC select shoes_id, brand, name, sale_price, rating, sum(1) as total_sales
-- MAGIC       from orders_transction
-- MAGIC       group by shoes_id, brand, name, sale_price, rating)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * customer feature

-- COMMAND ----------

select * FROM cust_click_stream_history

-- COMMAND ----------

-- most view and buy shoes brand
-- view time and they bougth

-- COMMAND ----------


CREATE OR REPLACE table  customer_interact_before_buy AS (
with a AS (
    SELECT 
    customer_id,
    order_id,
    product_id,
    DATE_FORMAT(ts, 'yyyy-MM-dd HH:mm:ss') as ts,
    LAG(DATE_FORMAT(ts, 'yyyy-MM-dd HH:mm:ss')) OVER (PARTITION BY customer_id ORDER BY ts ASC) AS prev_odr_ts
    FROM 
        shoe_orders
    GROUP BY 
        customer_id, order_id, product_id, ts
    ORDER BY 
        customer_id, ts DESC ),
  b AS (
    SELECT 
      cs.* 
    FROM cust_click_stream_history cs
    ),
  c AS (
    select a.*,b.* 
    from a 
    join b on a.product_id = b.shoes_id and a.customer_id = b.user_id 
    --where a.ts > b.shoe_clickstream_ts
    )
    SELECT 
    customer_id,
    order_id, 
    sum (case when shoe_clickstream_ts between  COALESCE(prev_odr_ts, '1900-01-01')  and ts then 1 else 0 end ) as interact_count_before_buy, -- sum customer that interaction time with product before buy
    sum (case when shoe_clickstream_ts between  COALESCE(prev_odr_ts, '1900-01-01')  and ts then view_time else 0 end ) as view_duration_before_buy
  from c group by customer_id,order_id
)


-- COMMAND ----------

-- SELECT 
--     customer_id,
--     order_id,
--     product_id,
--     DATE_FORMAT(ts, 'yyyy-MM-dd HH:mm:ss') as ts,
--     LAG(DATE_FORMAT(ts, 'yyyy-MM-dd HH:mm:ss')) OVER (PARTITION BY customer_id ORDER BY ts ASC) AS prev_odr_ts
-- FROM 
--     shoe_orders
-- GROUP BY 
--     customer_id, order_id,product_id, ts
-- ORDER BY 
--     customer_id, ts DESC

-- COMMAND ----------

-- with a AS (
--     SELECT 
--     customer_id,order_id,ts, LAG(ts) OVER (ORDER BY ts) AS prev_odr_ts
--     FROM shoe_orders so  group by customer_id,order_id,ts),
--   b AS (
--     SELECT 
--       cs.* 
--     FROM cust_click_stream_history cs
--     ),
--   c AS (
--     select a.*,b.* 
--     from a 
--     join b on a.product_id = b.shoes_id and a.customer_id = b.user_id 
--     --where a.ts > b.shoe_clickstream_ts
--     )
-- SELECT order_id,ts,prev_odr_ts,shoe_clickstream_ts from c where customer_id = '3e473d17-edeb-4d0e-9acf-c1441326d119' and shoe_clickstream_ts between  COALESCE(prev_odr_ts, '1900-01-01')  and ts

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC 
-- MAGIC columns_statement = get_col_name(table_name = ['customers'],perfix = ['c'] ,except_col = ['arrive_dt'])
-- MAGIC print(columns_statement)

-- COMMAND ----------

CREATE OR REPLACE table  customer_feature AS (
with 
  click_info as ( 
      SELECT 
        ch.user_id,
        ch.brand AS most_interaction_brand,
        ROW_NUMBER() OVER (PARTITION BY ch.user_id ORDER BY average_view_duration DESC) AS row_num
      FROM (
        SELECT 
          user_id,
          brand,
          SUM(1) AS view_count,
          SUM(view_time) AS total_view_times,
          SUM(view_time)/sum(1) as average_view_duration
        FROM 
          cust_click_stream_history
        GROUP BY 
          user_id,
          brand
      ) ch
        ),
  enrich_click_most_interest_brand as (  --find to user interest product
        SELECT  
         *
        FROM click_info 
        PIVOT (
          min(most_interaction_brand)
          FOR row_num in (
           1 most_interaction_brand,2 second_interaction_brand
          )
        )
        ORDER BY user_id DESC
               ),
  click_before_buy as (
    -- calulat avg view duration before buy items
      SELECT 
        customer_id, 
        SUM(view_duration_before_buy) / SUM(interact_count_before_buy) AS avg_view_duration_before_buy
      FROM customer_interact_before_buy 
      GROUP BY customer_id
  )
select cb.*,eb.*, c.first_name, c.last_name, c.email, c.phone, c.street_address, c.state, c.zip_code, c.country, c.country_code
from click_before_buy cb 
left join enrich_click_most_interest_brand eb on cb.customer_id = eb.user_id
left join customers c on cb.customer_id = c.id
)
--select * from enrich_click_most_interest_brand

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * click monitor -> find a customer who just clicked on our page within the last 15 minutes

-- COMMAND ----------


CREATE OR REPLACE temp view real_time_click_stream_15_minutes AS (
SELECT * 
from cust_click_stream_history ch 
where ch.ts  >= (current_timestamp() - INTERVAL 15 MINUTES))

-- COMMAND ----------


