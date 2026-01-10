--SILVER LAYER CREATED FOR ORDERS TABLE 
CREATE OR REPLACE TABLE orders_clean AS 
    SELECT  order_id,customer_id, product_id
    order_ts, order_amount,
    UPPER(order_status) AS order_status,
    CURRENT_TIMESTAMP() AS _processed_at_
    FROM DATAXSNOWFLAKE.BRONZE.ORDERS_RAW

-- DEDUPLICATION OF DATA
CREATE OR REPLACE TABLE orders_dedup AS 
SELECT * FROM (
SELECT * ,
ROW_NUMBER() OVER(PARTITION BY order_id ORDER BY order_ts DESC) rn
FROM orders_clean
)
Where rn=1


    
CREATE OR REPLACE STREAM st_orders
ON TABLE orders_dedup
APPEND_ONLY=TRUE;

select * from st_orders;

