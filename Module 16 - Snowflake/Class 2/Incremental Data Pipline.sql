--Incremental Data Pipleine Creation Task
CREATE OR REPLACE TASK task_merge_orders
WAREHOUSE=COMPUTE_WH
SCHEDULE='1 MINUTE'
AS
MERGE INTO gold.fact_orders t
USING SILVER.st_orders s
ON t.order_id=s.order_id
WHEN MATCHED THEN UPDATE SET
t.order_status=s.order_status,
t._loaded_at=CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN 
INSERT VALUES(
s.order_id,s.customer_id,DATE(s.order_ts),s.order_ts,
s.order_amount,s.order_status,CURRENT_TIMESTAMP()
);

ALTER TASK task_merge_orders RESUME;

SELECT * FROM gold.fact_orders
SHOW TASKS LIKE 'TASK_MERGE_ORDERS';


-- TO DEBUG THE TASK ERROR TRIGGER
SELECT *
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
WHERE NAME ILIKE '%TASK_MERGE_ORDERS%'
ORDER BY SCHEDULED_TIME DESC
LIMIT 10;
2026-01-10 01:17:01.254 -0800

--DDL GOLD FACT TABLE 
CREATE OR REPLACE TABLE fact_orders (
  order_id       NUMBER,
  customer_id    STRING,
  order_date     DATE,
  order_ts       STRING,
  order_amount   STRING,
  order_status   STRING,
  _loaded_at     STRING
);