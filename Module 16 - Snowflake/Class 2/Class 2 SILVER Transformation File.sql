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
APPEND_ONLY=FALSE;

select * from st_orders;

select * from orders_dedup

INSERT INTO orders_dedup (
    ORDER_ID,
    CUSTOMER_ID,
    ORDER_TS,
    ORDER_AMOUNT,
    ORDER_STATUS,
    _PROCESSED_AT_,
    RN
)
VALUES
    (10061, 'C004', 'P104', 1599, 'PLACED',     CURRENT_TIMESTAMP, 1),
    (10071, 'C005', 'P105',  799, 'PLACED',     CURRENT_TIMESTAMP, 1),
    (10081, 'C002', 'P106', 2499, 'SHIPPED',    CURRENT_TIMESTAMP, 1),
    (10011, 'C003', 'P107',  499, 'CANCELLED',  CURRENT_TIMESTAMP, 1),
    (1011, 'C001', 'P108', 1899, 'DELIVERED',  CURRENT_TIMESTAMP, 1);

