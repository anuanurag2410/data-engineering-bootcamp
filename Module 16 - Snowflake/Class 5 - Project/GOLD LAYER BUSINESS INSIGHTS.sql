USE DATABASE ZOMATO;
USE SCHEMA GOLD;

CREATE OR REPLACE TABLE order_revenue_fact(
order_id NUMBER,
customer_id STRING, 
city STRING, 
order_status STRING, 
items_total NUMBER(10,2),
payment_amount NUMBER(10,2),
refund_amount NUMBER(10,2),
net_revenue NUMBER(10,2)
)


-- doing Initial backfill on the GOLD Table 

INSERT INTO order_revenue_fact
SELECT 
o.order_id,
o.customer_id,
o.city,
o.order_status,
COALESCE(a.item_total,0),
COALESCE(p.pay,0),
COALESCE(r.refund,0),
COALESCE(p.pay,0)-COALESCE(r.refund,0),

FROM silver.orders_clean as o 
LEFT JOIN silver.order_amount  as a 
ON o.order_id=a.order_id 
LEFT JOIN (

SELECT order_id,SUM(amount) as Pay
FROM silver.payments_flat
WHERE event_type='PAYMENT_SUCCESS'
GROUP BY order_id
) as p 
ON o.ORDER_ID=p.ORDER_ID
LEFT JOIN 
(
SELECT order_id,SUM(refund_amount) as refund
FROM bronze.refunds_raw
GROUP BY order_id
)as r 
ON o.order_id=r.ORDER_ID

PRIMARY KEY 

