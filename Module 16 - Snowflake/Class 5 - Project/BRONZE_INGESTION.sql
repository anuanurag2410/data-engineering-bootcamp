--Create wareshouse command 
CREATE OR REPLACE WAREHOUSE wh_ingest_xs
WAREHOUSE_SIZE='XSMALL'
AUTO_SUSPEND=60
AUTO_RESUME=TRUE;

-- Drop WAREHOUSE Command 
DROP WAREHOUSE wh_ingest_xs


USE DATABASE ZOMATO;
USE SCHEMA BRONZE;

--Create the File format for CSV and JSON
CREATE OR REPLACE FILE FORMAT ff_csv
TYPE=CSV SKIP_HEADER=1 FIELD_DELIMITER=',' TRIM_SPACE=TRUE;

CREATE OR REPLACE FILE FORMAT ff_json TYPE=JSON;

--CREATE Stages 
CREATE OR REPLACE STAGE stg_orders FILE_FORMAT=ff_csv;
CREATE OR REPLACE STAGE stg_items FILE_FORMAT=ff_csv;
CREATE OR REPLACE STAGE stg_payments FILE_FORMAT=ff_json;
CREATE OR REPLACE STAGE stg_refunds FILE_FORMAT=ff_csv;


--Create the Bronze Tables 
-- Orders_raw 
CREATE OR REPLACE TABLE orders_raw (
  order_id NUMBER,
  customer_id STRING,
  order_ts STRING,
  city STRING,
  order_status STRING
);


-- Orders items raw 
CREATE OR REPLACE TABLE order_items_raw (
  order_id NUMBER,
  item_id STRING,
  item_name STRING,
  qty NUMBER,
  price NUMBER(10,2)
);


-- Payments Event raw 
CREATE OR REPLACE TABLE payment_events_raw (
  raw VARIANT
);

-- Refunds  raw 
CREATE OR REPLACE TABLE refunds_raw (
  refund_id STRING,
  order_id NUMBER,
  refund_ts TIMESTAMP_NTZ,
  refund_amount NUMBER(10,2),
  reason STRING
);


--Now we will load the Data using copy into from STAGGING layer to BRONZE Layer
COPY INTO orders_raw FROM @stg_orders ON_ERROR='CONTINUE';
COPY INTO order_items_raw FROM @stg_items ON_ERROR='CONTINUE';
COPY INTO refunds_raw FROM @stg_refunds ON_ERROR='CONTINUE';

COPY INTO PAYMENT_EVENTS_RAW(raw)
FROM (SELECT $1 FROM @stg_payments)
FILE_FORMAT=(TYPE=JSON)
ON_ERROR='CONTINUE';








