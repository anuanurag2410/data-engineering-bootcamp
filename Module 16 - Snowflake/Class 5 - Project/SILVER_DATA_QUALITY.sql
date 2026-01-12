USE DATABASE ZOMATO;
USE SCHEMA SILVER;

CREATE OR REPLACE TABLE dq_audit(
  check_ts TIMESTAMP_NTZ,
  check_name STRING,
  failed_rows NUMBER,
  severity STRING,
  explanation STRING
);


INSERT INTO dq_audit
SELECT 
CURRENT_TIMESTAMP(),
'orders_null_order_id_check',
count(*),
'CRITICAL',
'ORDER_ID is business key and definetely it cannot be null as it will break joins'
FROM ZOMATO.bronze.orders_raw
WHERE order_id is NULL



-- Silver Order CLean Table 
CREATE OR REPLACE TABLE orders_clean as 
SELECT order_id,customer_id, order_ts,city,
UPPER(order_status)AS order_status,
FROM bronze.orders_raw;

-- ITEM AGGREGATION 
CREATE OR REPLACE TABLE order_amount as 
SELECT 
order_id, 
sum(qty*price)as item_total, 
CURRENT_TIMESTAMP as processed_at
FROM bronze.order_items_raw
group by order_id;

--JSON flatten 
CREATE OR REPLACE TABLE payments_flat
As 
SELECT 
p.value:order_id::NUMBER AS order_id,
p.value:event_type::STRING AS event_type,
p.value:amount::NUMBER(10,2) AS amount,
CURRENT_TIMESTAMP() as processed_at
FROM 
bronze.payment_events_raw r,
LATERAL FLATTEN(input=> r.raw) p

