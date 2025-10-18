-- BigQuery INSERTS for Omni-Commerce ODS synthetic data
-- Project: `datamigrationproject-475415`  Dataset: `ods`
-- Generated: 2025-10-17T15:51:59.337491Z
-- Each table gets >= the requested number of rows.
-- Order ensures FK parents are populated before children.

INSERT INTO `datamigrationproject-475415.ods.ods_categories`
  (category_id, category_name, parent_category_id, created_at, updated_at)
SELECT
  id AS category_id,
  CONCAT('Category ', id) AS category_name,
  NULL AS parent_category_id, -- simpler since both CASE branches were NULL
  TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL MOD(id * 7, 365 * 24 * 60) MINUTE) AS created_at,
  TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL MOD((id + 7), 365 * 24 * 60) MINUTE) AS updated_at
FROM UNNEST(GENERATE_ARRAY(1, 1000)) AS id;

INSERT INTO `datamigrationproject-475415.ods.ods_sub_categories` (sub_category_id, category_id, sub_category_name, created_at, updated_at)
SELECT id AS sub_category_id,
       1 + MOD(id, 1000) AS category_id,
       CONCAT('SubCategory ', id),
       TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL MOD(id*7, 365*24*60) MINUTE), TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL MOD(id+1*7, 365*24*60) MINUTE)
FROM UNNEST(GENERATE_ARRAY(1, 2000)) AS id;

INSERT INTO `datamigrationproject-475415.ods.ods_plans` (plan_id, plan_name, price, billing_cycle, currency, status)
SELECT id AS plan_id,
       CONCAT('Plan ', id),
       CAST(ROUND(10 + RAND()*90,2) AS NUMERIC),
       CASE WHEN MOD(id,3)=0 THEN 'monthly' WHEN MOD(id,3)=1 THEN 'quarterly' ELSE 'yearly' END,
       'INR',
       CASE WHEN MOD(id,2)=0 THEN 'ACTIVE' ELSE 'INACTIVE' END
FROM UNNEST(GENERATE_ARRAY(1, 1200)) AS id;

INSERT INTO `datamigrationproject-475415.ods.ods_gl_accounts` (gl_account_id, gl_code, gl_name, gl_type)
SELECT id AS gl_account_id,
       CONCAT('GL', FORMAT('%04d', id)),
       CONCAT('GL Name ', id),
       CASE WHEN MOD(id,3)=0 THEN 'REVENUE' WHEN MOD(id,3)=1 THEN 'EXPENSE' ELSE 'LIABILITY' END
FROM UNNEST(GENERATE_ARRAY(1, 1200)) AS id;

INSERT INTO `datamigrationproject-475415.ods.ods_suppliers` (supplier_id, supplier_name, contact_email, phone, country, active, created_at, updated_at)
SELECT id AS supplier_id,
       CONCAT('Supplier ', id),
       CONCAT('supplier', id, '@example.com'),
       CONCAT('+91-98', FORMAT('%08d', id)),
       'IN',
       MOD(id,2)=0,
       TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL MOD(id*7, 365*24*60) MINUTE), TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL MOD(id+1*7, 365*24*60) MINUTE)
FROM UNNEST(GENERATE_ARRAY(1, 1000)) AS id;

INSERT INTO `datamigrationproject-475415.ods.ods_warehouses`
  (warehouse_id, warehouse_name, location_city, location_state, country, capacity, created_at, updated_at)
SELECT
  id AS warehouse_id,
  CONCAT('Warehouse ', CAST(id AS STRING)) AS warehouse_name,
  CONCAT('City ', CAST(id AS STRING)) AS location_city,
  CONCAT('State ', CAST(MOD(id, 30) AS STRING)) AS location_state,
  'IN' AS country,
  CAST(10000 + MOD(id, 5000) AS INT64) AS capacity,
  TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL MOD(id * 7, 365 * 24 * 60) MINUTE) AS created_at,
  TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL MOD((id + 7), 365 * 24 * 60) MINUTE) AS updated_at
FROM UNNEST(GENERATE_ARRAY(1, 1000)) AS id;


INSERT INTO `datamigrationproject-475415.ods.ods_stores`
  (store_id, store_code, store_name, city, state, country, opened_at, closed_at)
SELECT
  id AS store_id,
  CONCAT('STR', FORMAT('%05d', id)) AS store_code,
  CONCAT('Store ', CAST(id AS STRING)) AS store_name,
  CONCAT('City ', CAST(id AS STRING)) AS city,
  CONCAT('State ', CAST(MOD(id, 30) AS STRING)) AS state,
  'IN' AS country,
  TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL MOD(id * 7, 365 * 24 * 60) MINUTE) AS opened_at,
  CASE
    WHEN MOD(id, 50) = 0
      THEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL MOD((id + 50000) * 7, 365 * 24 * 60) MINUTE)
    ELSE NULL
  END AS closed_at
FROM UNNEST(GENERATE_ARRAY(1, 2000)) AS id;


INSERT INTO `datamigrationproject-475415.ods.ods_customers` (customer_id, first_name, last_name, email, phone, birth_date, gender, created_at, updated_at)
SELECT id AS customer_id,
       CONCAT('First', id), CONCAT('Last', id),
       CONCAT('user', id, '@example.com'),
       CONCAT('+91-99', FORMAT('%08d', id)),
       DATE_SUB(CURRENT_DATE(), INTERVAL MOD(id*3, 365) DAY),
       CASE WHEN MOD(id,2)=0 THEN 'M' ELSE 'F' END,
       TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL MOD(id*7, 365*24*60) MINUTE), TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL MOD(id+1*7, 365*24*60) MINUTE)
FROM UNNEST(GENERATE_ARRAY(1, 5000)) AS id;

INSERT INTO `datamigrationproject-475415.ods.ods_customer_addresses` (address_id, customer_id, address_type, line1, line2, city, state, postal_code, country, is_primary, created_at, updated_at)
SELECT id AS address_id,
       1 + MOD(id, 5000) AS customer_id,
       CASE WHEN MOD(id,3)=0 THEN 'home' WHEN MOD(id,3)=1 THEN 'work' ELSE 'other' END,
       CONCAT('Line1 ', id), CONCAT('Line2 ', id),
       CONCAT('City ', MOD(id,200)), CONCAT('State ', MOD(id,30)),
       CONCAT('56', FORMAT('%04d', MOD(id,10000))),
       'IN',
       MOD(id,4)=0,
       TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL MOD(id*7, 365*24*60) MINUTE), TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL MOD(id+1*7, 365*24*60) MINUTE)
FROM UNNEST(GENERATE_ARRAY(1, 7000)) AS id;

INSERT INTO `datamigrationproject-475415.ods.ods_customer_preferences` (pref_id, customer_id, marketing_opt_in, preferred_channel, language, currency, created_at, updated_at)
SELECT id AS pref_id,
       1 + MOD(id, 5000) AS customer_id,
       MOD(id,2)=0,
       CASE WHEN MOD(id,5)=0 THEN 'Email' WHEN MOD(id,5)=1 THEN 'SMS' WHEN MOD(id,5)=2 THEN 'Push' WHEN MOD(id,5)=3 THEN 'WhatsApp' ELSE 'Call' END,
       'en',
       'INR',
       TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL MOD(id*7, 365*24*60) MINUTE), TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL MOD(id+1*7, 365*24*60) MINUTE)
FROM UNNEST(GENERATE_ARRAY(1, 5000)) AS id;

INSERT INTO `datamigrationproject-475415.ods.ods_loyalty_accounts` (loyalty_id, customer_id, program_tier, points_balance, enrolled_at, updated_at)
SELECT id AS loyalty_id,
       1 + MOD(id, 5000) AS customer_id,
       CASE WHEN MOD(id,4)=0 THEN 'Bronze' WHEN MOD(id,4)=1 THEN 'Silver' WHEN MOD(id,4)=2 THEN 'Gold' ELSE 'Platinum' END,
       CAST(MOD(id*37, 50000) AS INT64),
       TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL MOD(id*7, 365*24*60) MINUTE), TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL MOD(id+1*7, 365*24*60) MINUTE)
FROM UNNEST(GENERATE_ARRAY(1, 4000)) AS id;

INSERT INTO `datamigrationproject-475415.ods.ods_products` (product_id, sku, product_name, brand, category_id, sub_category_id, status, created_at, updated_at)
SELECT id AS product_id,
       CONCAT('SKU-', FORMAT('%06d', id)),
       CONCAT('Product ', id),
       CONCAT('Brand ', MOD(id,200)),
       1 + MOD(id, 1000) AS category_id,
       1 + MOD(id, 2000) AS sub_category_id,
       CASE WHEN MOD(id,5)=0 THEN 'DISCONTINUED' ELSE 'ACTIVE' END,
       TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL MOD(id*7, 365*24*60) MINUTE), TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL MOD(id+1*7, 365*24*60) MINUTE)
FROM UNNEST(GENERATE_ARRAY(1, 5000)) AS id;

INSERT INTO `datamigrationproject-475415.ods.ods_skus` (sku_id, product_id, sku, upc, model, color, size, weight, active, created_at, updated_at)
SELECT id AS sku_id,
       1 + MOD(id, 5000) AS product_id,
       CONCAT('SKU-', FORMAT('%06d', id)),
       CONCAT('UPC', FORMAT('%012d', id)),
       CONCAT('M', FORMAT('%04d', MOD(id,10000))),
       CONCAT('Color-', MOD(id,64)), CONCAT('Size-', MOD(id,20)),
       0.5 + MOD(id,500)/10.0,
       MOD(id,10)<>0,
       TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL MOD(id*7, 365*24*60) MINUTE), TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL MOD(id+1*7, 365*24*60) MINUTE)
FROM UNNEST(GENERATE_ARRAY(1, 8000)) AS id;

INSERT INTO `datamigrationproject-475415.ods.ods_price_lists` (price_list_id, name, currency, valid_from, valid_to, is_default, created_at, updated_at)
SELECT id AS price_list_id,
       CONCAT('PriceList ', id),
       'INR',
       DATE_SUB(CURRENT_DATE(), INTERVAL MOD(id*2, 365) DAY),
       NULL,
       MOD(id,50)=1,
       TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL MOD(id*7, 365*24*60) MINUTE), TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL MOD(id+1*7, 365*24*60) MINUTE)
FROM UNNEST(GENERATE_ARRAY(1, 1000)) AS id;

INSERT INTO `datamigrationproject-475415.ods.ods_prices` (price_id, price_list_id, sku_id, base_price, discount, final_price, valid_from, valid_to, updated_at)
SELECT id AS price_id,
       1 + MOD(id, 1000) AS price_list_id,
       1 + MOD(id*7, 8000) AS sku_id,
       CAST(ROUND(100 + RAND()*9000,2) AS NUMERIC) AS base_price,
       CAST(ROUND(MOD(id,30),2) AS NUMERIC) AS discount,
       CAST(ROUND((100 + RAND()*9000) - MOD(id,30),2) AS NUMERIC) AS final_price,
       DATE_SUB(CURRENT_DATE(), INTERVAL MOD(id*5, 365) DAY),
       NULL,
       TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL MOD(id*7, 365*24*60) MINUTE)
FROM UNNEST(GENERATE_ARRAY(1, 20000)) AS id;

INSERT INTO `datamigrationproject-475415.ods.ods_carts` (cart_id, customer_id, channel_id, created_at, abandoned_at)
SELECT id AS cart_id,
       1 + MOD(id, 5000) AS customer_id,
       1 + MOD(id, 1000) AS channel_id,
       TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL MOD(id*7, 365*24*60) MINUTE),
       CASE WHEN MOD(id,3)=0 THEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL MOD(id+1000*7, 365*24*60) MINUTE) ELSE NULL END
FROM UNNEST(GENERATE_ARRAY(1, 6000)) AS id;

INSERT INTO `datamigrationproject-475415.ods.ods_cart_items` (cart_item_id, cart_id, sku_id, qty, unit_price, added_at)
SELECT id AS cart_item_id,
       1 + MOD(id, 6000) AS cart_id,
       1 + MOD(id*11, 8000) AS sku_id,
       1 + MOD(id,5),
       CAST(ROUND(100 + RAND()*9000,2) AS NUMERIC),
       TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL MOD(id*7, 365*24*60) MINUTE)
FROM UNNEST(GENERATE_ARRAY(1, 12000)) AS id;

INSERT INTO `datamigrationproject-475415.ods.ods_orders`
  (order_id, customer_id, channel_id, store_id, order_number, order_ts, status, currency,
   subtotal, tax, shipping, total_amount, payment_status)
WITH base AS (
  SELECT
    id,
    1 + MOD(id * 13, 5000) AS customer_id,
    1 + MOD(id * 7, 1000)  AS channel_id,
    1 + MOD(id * 5, 2000)  AS store_id,
    CONCAT('ORD', FORMAT('%08d', id)) AS order_number,
    TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL MOD(id * 7, 365 * 24 * 60) MINUTE) AS order_ts,
    CASE
      WHEN MOD(id, 10) = 0 THEN 'CANCELLED'
      WHEN MOD(id, 10) = 1 THEN 'RETURNED'
      ELSE 'PLACED'
    END AS status,
    'INR' AS currency,

    -- Keep money as NUMERIC for math; avoid FLOAT64 in arithmetic.
    CAST(ROUND(200 + RAND() * 5000, 2) AS NUMERIC) AS subtotal_num,
    CAST(ROUND(50  + RAND() * 150 , 2) AS NUMERIC) AS shipping_num
  FROM UNNEST(GENERATE_ARRAY(1, 12000)) AS id
),
calc AS (
  SELECT
    *,
    -- Keep tax purely NUMERIC: use fraction instead of 0.18 (FLOAT64)
    ROUND((subtotal_num * 18) / 100, 2) AS tax_num,
    ROUND(subtotal_num + ((subtotal_num * 18) / 100) + shipping_num, 2) AS total_num
  FROM base
)
SELECT
  id AS order_id,
  customer_id,
  channel_id,
  store_id,
  order_number,
  order_ts,
  status,
  currency,
  -- subtotal column is STRING in your table
  FORMAT('%.2f', CAST(subtotal_num AS FLOAT64)) AS subtotal,
  -- tax/shipping/total_amount are NUMERIC
  tax_num AS tax,
  shipping_num AS shipping,
  total_num AS total_amount,
  CASE WHEN MOD(id, 5) = 0 THEN 'PENDING' ELSE 'PAID' END AS payment_status
FROM calc;


INSERT INTO `datamigrationproject-475415.ods.ods_order_items`
  (order_item_id, order_id, sku_id, qty, unit_price, discount, line_amount, fulfillment_status)
WITH base AS (
  SELECT
    id,
    1 + MOD(id, 12000) AS order_id,
    1 + MOD(id * 17, 8000) AS sku_id,
    1 + MOD(id, 5) AS qty,
    CAST(ROUND(100 + RAND() * 9000, 2) AS NUMERIC) AS unit_price,
    CAST(ROUND(MOD(id, 25), 2) AS NUMERIC) AS discount,
    CASE
      WHEN MOD(id, 4) = 0 THEN 'BACKORDER'
      WHEN MOD(id, 4) = 1 THEN 'SHIPPED'
      WHEN MOD(id, 4) = 2 THEN 'DELIVERED'
      ELSE 'PENDING'
    END AS fulfillment_status
  FROM UNNEST(GENERATE_ARRAY(1, 36000)) AS id
)
SELECT
  id AS order_item_id,
  order_id,
  sku_id,
  qty,
  unit_price,
  discount,
  ROUND((unit_price * qty) - discount, 2) AS line_amount,
  fulfillment_status
FROM base;


INSERT INTO `datamigrationproject-475415.ods.ods_payments` (payment_id, order_id, payment_method, amount, provider, auth_code, payment_ts, status)
SELECT id AS payment_id,
       1 + MOD(id, 12000) AS order_id,
       CASE WHEN MOD(id,3)=0 THEN 'CARD' WHEN MOD(id,3)=1 THEN 'UPI' ELSE 'COD' END,
       CAST(ROUND(200 + RAND()*5000,2) AS NUMERIC),
       'Razorpay',
       CONCAT('AUTH', FORMAT('%08d', id)),
       TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL MOD(id*7, 365*24*60) MINUTE),
       CASE WHEN MOD(id,7)=0 THEN 'FAILED' ELSE 'SUCCESS' END
FROM UNNEST(GENERATE_ARRAY(1, 12000)) AS id;

INSERT INTO `datamigrationproject-475415.ods.ods_refunds` (refund_id, payment_id, order_id, amount, reason, refund_ts, status)
SELECT id AS refund_id,
       1 + MOD(id, 12000) AS payment_id,
       1 + MOD(id, 12000) AS order_id,
       CAST(ROUND(RAND()*1000,2) AS NUMERIC),
       'Customer Request',
       TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL MOD(id*7, 365*24*60) MINUTE),
       CASE WHEN MOD(id,3)=0 THEN 'REJECTED' ELSE 'APPROVED' END
FROM UNNEST(GENERATE_ARRAY(1, 3000)) AS id;

INSERT INTO `datamigrationproject-475415.ods.ods_chargebacks` (chargeback_id, payment_id, order_id, amount, opened_ts, closed_ts, status)
SELECT id AS chargeback_id,
       1 + MOD(id, 12000) AS payment_id,
       1 + MOD(id, 12000) AS order_id,
       CAST(ROUND(RAND()*1000,2) AS NUMERIC),
       TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL MOD(id*7, 365*24*60) MINUTE), TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL MOD(id+10000*7, 365*24*60) MINUTE),
       CASE WHEN MOD(id,2)=0 THEN 'OPEN' ELSE 'CLOSED' END
FROM UNNEST(GENERATE_ARRAY(1, 1500)) AS id;

INSERT INTO `datamigrationproject-475415.ods.ods_shipments` (shipment_id, order_id, carrier, service_level, tracking_no, shipped_ts, delivered_ts, status, warehouse_id)
SELECT id AS shipment_id,
       1 + MOD(id, 12000) AS order_id,
       CASE WHEN MOD(id,3)=0 THEN 'BlueDart' WHEN MOD(id,3)=1 THEN 'Delhivery' ELSE 'EcomExpress' END,
       CASE WHEN MOD(id,3)=0 THEN 'STD' WHEN MOD(id,3)=1 THEN 'EXP' ELSE 'PRIME' END,
       CONCAT('TRK', FORMAT('%012d', id)),
       TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL MOD(id*7, 365*24*60) MINUTE),
       TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL MOD(id+5000*7, 365*24*60) MINUTE),
       CASE WHEN MOD(id,4)=0 THEN 'IN_TRANSIT' WHEN MOD(id,4)=1 THEN 'DELIVERED' WHEN MOD(id,4)=2 THEN 'DELAYED' ELSE 'CREATED' END,
       1 + MOD(id, 1000) AS warehouse_id
FROM UNNEST(GENERATE_ARRAY(1, 8000)) AS id;

INSERT INTO `datamigrationproject-475415.ods.ods_shipment_items` (shipment_item_id, shipment_id, order_item_id, qty_shipped)
SELECT id AS shipment_item_id,
       1 + MOD(id, 8000) AS shipment_id,
       1 + MOD(id, 36000) AS order_item_id,
       1 + MOD(id,5)
FROM UNNEST(GENERATE_ARRAY(1, 16000)) AS id;

INSERT INTO `datamigrationproject-475415.ods.ods_delivery_partners` (partner_id, partner_name, contact_email, phone, country, active)
SELECT id AS partner_id,
       CONCAT('Partner ', id),
       CONCAT('partner', id, '@example.com'),
       CONCAT('+91-97', FORMAT('%08d', id)),
       'IN',
       MOD(id,3)<>0
FROM UNNEST(GENERATE_ARRAY(1, 1000)) AS id;

INSERT INTO `datamigrationproject-475415.ods.ods_delivery_routes` (route_id, partner_id, shipment_id, route_seq, hub, arrival_ts, departure_ts)
SELECT id AS route_id,
       1 + MOD(id, 1000) AS partner_id,
       1 + MOD(id, 8000) AS shipment_id,
       MOD(id,5)+1 AS route_seq,
       CONCAT('Hub ', MOD(id,100)),
       TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL MOD(id*7, 365*24*60) MINUTE), TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL MOD(id+1000*7, 365*24*60) MINUTE)
FROM UNNEST(GENERATE_ARRAY(1, 8000)) AS id;

INSERT INTO `datamigrationproject-475415.ods.ods_delivery_events` (event_id, shipment_id, event_type, event_ts, location, notes)
SELECT id AS event_id,
       1 + MOD(id, 8000) AS shipment_id,
       CASE WHEN MOD(id,4)=0 THEN 'PICKED' WHEN MOD(id,4)=1 THEN 'IN_TRANSIT' WHEN MOD(id,4)=2 THEN 'OUT_FOR_DELIVERY' ELSE 'DELIVERED' END,
       TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL MOD(id*7, 365*24*60) MINUTE),
       CONCAT('Loc ', MOD(id,500)),
       CONCAT('Note ', id)
FROM UNNEST(GENERATE_ARRAY(1, 16000)) AS id;

INSERT INTO `datamigrationproject-475415.ods.ods_returns` (return_id, order_id, customer_id, initiated_ts, approved_ts, status, refund_id)
SELECT id AS return_id,
       1 + MOD(id, 12000) AS order_id,
       1 + MOD(id, 5000) AS customer_id,
       TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL MOD(id*7, 365*24*60) MINUTE), TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL MOD(id+500*7, 365*24*60) MINUTE),
       CASE WHEN MOD(id,4)=0 THEN 'REJECTED' WHEN MOD(id,4)=1 THEN 'APPROVED' ELSE 'PENDING' END,
       1 + MOD(id, 3000)
FROM UNNEST(GENERATE_ARRAY(1, 2500)) AS id;

INSERT INTO `datamigrationproject-475415.ods.ods_return_items` (return_item_id, return_id, order_item_id, qty, reason, condition, resolution)
SELECT id AS return_item_id,
       1 + MOD(id, 2500) AS return_id,
       1 + MOD(id, 36000) AS order_item_id,
       1 + MOD(id,3),
       'Defective',
       'Opened',
       CASE WHEN MOD(id,2)=0 THEN 'REFUND' ELSE 'REPLACE' END
FROM UNNEST(GENERATE_ARRAY(1, 5000)) AS id;

INSERT INTO `datamigrationproject-475415.ods.ods_support_agents` (agent_id, agent_name, email, team, location, active_since)
SELECT id AS agent_id,
       CONCAT('Agent ', id),
       CONCAT('agent', id, '@example.com'),
       CONCAT('Team ', MOD(id,10)),
       CONCAT('Loc ', MOD(id,50)),
       TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL MOD(id*7, 365*24*60) MINUTE)
FROM UNNEST(GENERATE_ARRAY(1, 1200)) AS id;

INSERT INTO `datamigrationproject-475415.ods.ods_support_tickets` (ticket_id, customer_id, order_id, opened_ts, closed_ts, priority, status, channel)
SELECT id AS ticket_id,
       1 + MOD(id, 5000) AS customer_id,
       1 + MOD(id, 12000) AS order_id,
       TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL MOD(id*7, 365*24*60) MINUTE), TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL MOD(id+2000*7, 365*24*60) MINUTE),
       CASE WHEN MOD(id,3)=0 THEN 'HIGH' WHEN MOD(id,3)=1 THEN 'MEDIUM' ELSE 'LOW' END,
       CASE WHEN MOD(id,4)=0 THEN 'OPEN' WHEN MOD(id,4)=1 THEN 'IN_PROGRESS' WHEN MOD(id,4)=2 THEN 'RESOLVED' ELSE 'CLOSED' END,
       CASE WHEN MOD(id,3)=0 THEN 'Email' WHEN MOD(id,3)=1 THEN 'Phone' ELSE 'Chat' END
FROM UNNEST(GENERATE_ARRAY(1, 6000)) AS id;

INSERT INTO `datamigrationproject-475415.ods.ods_support_interactions` (interaction_id, ticket_id, agent_id, interaction_ts, medium, notes)
SELECT id AS interaction_id,
       1 + MOD(id, 6000) AS ticket_id,
       1 + MOD(id, 1200) AS agent_id,
       TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL MOD(id*7, 365*24*60) MINUTE),
       CASE WHEN MOD(id,3)=0 THEN 'Email' WHEN MOD(id,3)=1 THEN 'Call' ELSE 'Chat' END,
       CONCAT('Notes ', id)
FROM UNNEST(GENERATE_ARRAY(1, 12000)) AS id;

INSERT INTO `datamigrationproject-475415.ods.ods_marketing_campaigns` (campaign_id, campaign_name, channel_id, start_date, end_date, budget, currency, status)
SELECT id AS campaign_id,
       CONCAT('Campaign ', id),
       1 + MOD(id, 1000) AS channel_id,
       DATE_SUB(CURRENT_DATE(), INTERVAL MOD(id*2, 365) DAY),
       NULL,
       CAST(ROUND(10000 + RAND()*900000,2) AS NUMERIC),
       'INR',
       CASE WHEN MOD(id,4)=0 THEN 'PAUSED' ELSE 'ACTIVE' END
FROM UNNEST(GENERATE_ARRAY(1, 1500)) AS id;

INSERT INTO `datamigrationproject-475415.ods.ods_marketing_adgroups` (adgroup_id, campaign_id, adgroup_name, status, created_at)
SELECT id AS adgroup_id,
       1 + MOD(id, 1500) AS campaign_id,
       CONCAT('AdGroup ', id),
       CASE WHEN MOD(id,3)=0 THEN 'PAUSED' ELSE 'ACTIVE' END,
       TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL MOD(id*7, 365*24*60) MINUTE)
FROM UNNEST(GENERATE_ARRAY(1, 3000)) AS id;

INSERT INTO `datamigrationproject-475415.ods.ods_marketing_ads` (ad_id, adgroup_id, ad_name, format, status, created_at)
SELECT id AS ad_id,
       1 + MOD(id, 3000) AS adgroup_id,
       CONCAT('Ad ', id),
       CASE WHEN MOD(id,3)=0 THEN 'VIDEO' WHEN MOD(id,3)=1 THEN 'IMAGE' ELSE 'TEXT' END,
       CASE WHEN MOD(id,4)=0 THEN 'PAUSED' ELSE 'ACTIVE' END,
       TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL MOD(id*7, 365*24*60) MINUTE)
FROM UNNEST(GENERATE_ARRAY(1, 6000)) AS id;

INSERT INTO `datamigrationproject-475415.ods.ods_ad_impressions` (impression_id, ad_id, impression_ts, device, geo, placement)
SELECT id AS impression_id,
       1 + MOD(id, 6000) AS ad_id,
       TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL MOD(id*7, 365*24*60) MINUTE),
       CASE WHEN MOD(id,3)=0 THEN 'mobile' WHEN MOD(id,3)=1 THEN 'desktop' ELSE 'tablet' END,
       CONCAT('IN-', FORMAT('%02d', MOD(id,36))),
       CASE WHEN MOD(id,3)=0 THEN 'feed' WHEN MOD(id,3)=1 THEN 'search' ELSE 'video' END
FROM UNNEST(GENERATE_ARRAY(1, 60000)) AS id;

INSERT INTO `datamigrationproject-475415.ods.ods_ad_clicks` (click_id, ad_id, click_ts, device, geo, placement, utm_campaign, utm_medium, utm_source)
SELECT id AS click_id,
       1 + MOD(id, 6000) AS ad_id,
       TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL MOD(id*7, 365*24*60) MINUTE),
       CASE WHEN MOD(id,3)=0 THEN 'mobile' WHEN MOD(id,3)=1 THEN 'desktop' ELSE 'tablet' END,
       CONCAT('IN-', FORMAT('%02d', MOD(id,36))),
       CASE WHEN MOD(id,3)=0 THEN 'feed' WHEN MOD(id,3)=1 THEN 'search' ELSE 'video' END,
       'spring_sale','cpc','google'
FROM UNNEST(GENERATE_ARRAY(1, 15000)) AS id;

INSERT INTO `datamigrationproject-475415.ods.ods_web_sessions` (session_id, customer_id, session_start_ts, device, geo, source, medium, campaign)
SELECT id AS session_id,
       1 + MOD(id, 5000) AS customer_id,
       TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL MOD(id*7, 365*24*60) MINUTE),
       CASE WHEN MOD(id,3)=0 THEN 'mobile' WHEN MOD(id,3)=1 THEN 'desktop' ELSE 'tablet' END,
       CONCAT('IN-', FORMAT('%02d', MOD(id,36))),
       'google','cpc','spring_sale'
FROM UNNEST(GENERATE_ARRAY(1, 12000)) AS id;

INSERT INTO `datamigrationproject-475415.ods.ods_web_events`
  (event_id, session_id, event_name, event_ts, page_url, product_id, cart_id, order_id)
WITH base AS (
  SELECT
    id,
    1 + MOD(id, 12000) AS session_id,
    CASE
      WHEN MOD(id, 4) = 0 THEN 'page_view'
      WHEN MOD(id, 4) = 1 THEN 'add_to_cart'
      WHEN MOD(id, 4) = 2 THEN 'checkout'
      ELSE 'purchase'
    END AS event_name,
    TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL MOD(id * 7, 365 * 24 * 60) MINUTE) AS event_ts,
    CONCAT('https://example.com/p/', CAST(1 + MOD(id, 5000) AS STRING)) AS page_url,
    1 + MOD(id, 6000) AS product_id,
    1 + MOD(id, 12000) AS cart_id
  FROM UNNEST(GENERATE_ARRAY(1, 36000)) AS id
)
SELECT
  id AS event_id,
  session_id,
  event_name,
  event_ts,
  page_url,
  product_id,
  cart_id,
  CASE WHEN event_name = 'purchase' THEN 1 + MOD(id, 12000) ELSE NULL END AS order_id
FROM base;


INSERT INTO `datamigrationproject-475415.ods.ods_inventory` (inventory_id, sku_id, warehouse_id, on_hand, allocated, available, safety_stock, as_of_ts)
SELECT id AS inventory_id,
       1 + MOD(id, 8000) AS sku_id,
       1 + MOD(id, 1000) AS warehouse_id,
       CAST(50 + MOD(id,500) AS INT64) AS on_hand,
       CAST(MOD(id,30) AS INT64) AS allocated,
       CAST(50 + MOD(id,500) - MOD(id,30) AS INT64) AS available,
       CAST(10 + MOD(id,20) AS INT64) AS safety_stock,
       TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL MOD(id*7, 365*24*60) MINUTE)
FROM UNNEST(GENERATE_ARRAY(1, 20000)) AS id;

INSERT INTO `datamigrationproject-475415.ods.ods_stock_movements` (movement_id, sku_id, warehouse_id, source_type, source_id, qty, reason, txn_ts)
SELECT id AS movement_id,
       1 + MOD(id, 8000) AS sku_id,
       1 + MOD(id, 1000) AS warehouse_id,
       CASE WHEN MOD(id,2)=0 THEN 'PO' ELSE 'ORDER' END AS source_type,
       CASE WHEN MOD(id,2)=0 THEN 1 + MOD(id, 5000) ELSE 1 + MOD(id, 12000) END AS source_id,
       CAST(1 + MOD(id,10) AS INT64) AS qty,
       CASE WHEN MOD(id,2)=0 THEN 'RECEIPT' ELSE 'ISSUE' END AS reason,
       TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL MOD(id*7, 365*24*60) MINUTE)
FROM UNNEST(GENERATE_ARRAY(1, 15000)) AS id;

INSERT INTO `datamigrationproject-475415.ods.ods_purchase_orders` (po_id, supplier_id, warehouse_id, po_number, status, order_date, expected_date, total_amount, currency, created_at, updated_at)
SELECT id AS po_id,
       1 + MOD(id, 1000) AS supplier_id,
       1 + MOD(id, 1000) AS warehouse_id,
       CONCAT('PO', FORMAT('%08d', id)),
       CASE WHEN MOD(id,4)=0 THEN 'OPEN' WHEN MOD(id,4)=1 THEN 'PARTIAL' WHEN MOD(id,4)=2 THEN 'CLOSED' ELSE 'CANCELLED' END,
       DATE_SUB(CURRENT_DATE(), INTERVAL MOD(id, 365) DAY),
       DATE_SUB(CURRENT_DATE(), INTERVAL MOD(id+7, 365) DAY),
       CAST(ROUND(1000 + RAND()*90000,2) AS NUMERIC),
       'INR',
       TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL MOD(id*7, 365*24*60) MINUTE), TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL MOD(id+1*7, 365*24*60) MINUTE)
FROM UNNEST(GENERATE_ARRAY(1, 5000)) AS id;

INSERT INTO `datamigrationproject-475415.ods.ods_purchase_order_items` (po_item_id, po_id, sku_id, qty_ordered, unit_cost, qty_received, created_at, updated_at)
SELECT id AS po_item_id,
       1 + MOD(id, 5000) AS po_id,
       1 + MOD(id, 8000) AS sku_id,
       CAST(1 + MOD(id,20) AS INT64),
       CAST(ROUND(50 + RAND()*500,2) AS NUMERIC),
       CAST(MOD(id,20) AS INT64),
       TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL MOD(id*7, 365*24*60) MINUTE), TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL MOD(id+1*7, 365*24*60) MINUTE)
FROM UNNEST(GENERATE_ARRAY(1, 15000)) AS id;

INSERT INTO `datamigrationproject-475415.ods.ods_invoices` (invoice_id, order_id, invoice_no, invoice_ts, amount_due, amount_paid, status, currency)
SELECT id AS invoice_id,
       1 + MOD(id, 12000) AS order_id,
       CONCAT('INV', FORMAT('%08d', id)),
       TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL MOD(id*7, 365*24*60) MINUTE),
       CAST(ROUND(100 + RAND()*1000,2) AS NUMERIC),
       CAST(ROUND(RAND()*1000,2) AS NUMERIC),
       CASE WHEN MOD(id,3)=0 THEN 'OPEN' WHEN MOD(id,3)=1 THEN 'PAID' ELSE 'PARTIAL' END,
       'INR'
FROM UNNEST(GENERATE_ARRAY(1, 12000)) AS id;

INSERT INTO `datamigrationproject-475415.ods.ods_invoice_lines`
  (invoice_line_id, invoice_id, sku_id, qty, unit_price, line_total)
WITH base AS (
  SELECT
    id,
    1 + MOD(id, 12000) AS invoice_id,
    1 + MOD(id * 19, 8000) AS sku_id,
    CAST(1 + MOD(id, 5) AS INT64) AS qty,
    CAST(ROUND(100 + RAND() * 9000, 2) AS NUMERIC) AS unit_price
  FROM UNNEST(GENERATE_ARRAY(1, 36000)) AS id
)
SELECT
  id AS invoice_line_id,
  invoice_id,
  sku_id,
  qty,
  unit_price,
  ROUND(qty * unit_price, 2) AS line_total
FROM base;


INSERT INTO `datamigrationproject-475415.ods.ods_financial_postings` (posting_id, gl_account_id, order_id, amount, posting_ts, dimension1, dimension2)
SELECT id AS posting_id,
       1 + MOD(id, 1200) AS gl_account_id,
       1 + MOD(id, 12000) AS order_id,
       CAST(ROUND(RAND()*2000,2) AS NUMERIC),
       TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL MOD(id*7, 365*24*60) MINUTE),
       CONCAT('dim1-', MOD(id,10)), CONCAT('dim2-', MOD(id,10))
FROM UNNEST(GENERATE_ARRAY(1, 15000)) AS id;

INSERT INTO `datamigrationproject-475415.ods.ods_promotions` (promo_id, promo_code, description, start_date, end_date, discount_type, discount_value, status)
SELECT id AS promo_id,
       CONCAT('PROMO', FORMAT('%05d', id)),
       CONCAT('Promo ', id),
       DATE_SUB(CURRENT_DATE(), INTERVAL MOD(id, 365) DAY),
       DATE_SUB(CURRENT_DATE(), INTERVAL MOD(id+14, 365) DAY),
       CASE WHEN MOD(id,2)=0 THEN 'PERCENT' ELSE 'AMOUNT' END,
       CAST(ROUND(5 + MOD(id,30),2) AS NUMERIC),
       CASE WHEN MOD(id,4)=0 THEN 'EXPIRED' ELSE 'ACTIVE' END
FROM UNNEST(GENERATE_ARRAY(1, 1200)) AS id;

INSERT INTO `datamigrationproject-475415.ods.ods_promo_eligibility` (promo_elig_id, promo_id, customer_id, eligible_from, eligible_to, used_flag)
SELECT id AS promo_elig_id,
       1 + MOD(id, 1200) AS promo_id,
       1 + MOD(id, 5000) AS customer_id,
       DATE_SUB(CURRENT_DATE(), INTERVAL MOD(id, 365) DAY), DATE_SUB(CURRENT_DATE(), INTERVAL MOD(id+30, 365) DAY),
       MOD(id,4)=0
FROM UNNEST(GENERATE_ARRAY(1, 6000)) AS id;

INSERT INTO `datamigrationproject-475415.ods.ods_promo_redemptions` (promo_red_id, promo_id, order_id, redeemed_ts, discount_amount)
SELECT id AS promo_red_id,
       1 + MOD(id, 1200) AS promo_id,
       1 + MOD(id, 12000) AS order_id,
       TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL MOD(id*7, 365*24*60) MINUTE),
       CAST(ROUND(10 + MOD(id,100),2) AS NUMERIC)
FROM UNNEST(GENERATE_ARRAY(1, 3000)) AS id;

INSERT INTO `datamigrationproject-475415.ods.ods_subscriptions` (subscription_id, customer_id, plan_id, start_date, end_date, status, auto_renew)
SELECT id AS subscription_id,
       1 + MOD(id, 5000) AS customer_id,
       1 + MOD(id, 1200) AS plan_id,
       DATE_SUB(CURRENT_DATE(), INTERVAL MOD(id, 365) DAY), NULL,
       CASE WHEN MOD(id,4)=0 THEN 'CANCELLED' ELSE 'ACTIVE' END,
       MOD(id,3)<>0
FROM UNNEST(GENERATE_ARRAY(1, 4000)) AS id;

INSERT INTO `datamigrationproject-475415.ods.ods_subscription_events` (sub_event_id, subscription_id, event_type, event_ts, notes)
SELECT id AS sub_event_id,
       1 + MOD(id, 4000) AS subscription_id,
       CASE WHEN MOD(id,4)=0 THEN 'CREATED' WHEN MOD(id,4)=1 THEN 'RENEWED' WHEN MOD(id,4)=2 THEN 'UPGRADED' ELSE 'CANCELLED' END,
       TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL MOD(id*7, 365*24*60) MINUTE),
       CONCAT('Note ', id)
FROM UNNEST(GENERATE_ARRAY(1, 8000)) AS id;

INSERT INTO `datamigrationproject-475415.ods.ods_reviews` (review_id, product_id, customer_id, rating, review_text, review_ts, helpful_votes)
SELECT id AS review_id,
       1 + MOD(id, 5000) AS product_id,
       1 + MOD(id, 5000) AS customer_id,
       CAST(1 + MOD(id,5) AS INT64),
       CONCAT('Review text ', id),
       TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL MOD(id*7, 365*24*60) MINUTE),
       CAST(MOD(id,50) AS INT64)
FROM UNNEST(GENERATE_ARRAY(1, 6000)) AS id;

INSERT INTO `datamigrationproject-475415.ods.ods_qna` (qna_id, product_id, customer_id, question, answer, asked_ts, answered_ts)
SELECT id AS qna_id,
       1 + MOD(id, 5000) AS product_id,
       1 + MOD(id, 5000) AS customer_id,
       CONCAT('Question ', id),
       CONCAT('Answer ', id),
       TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL MOD(id*7, 365*24*60) MINUTE), TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL MOD(id+1000*7, 365*24*60) MINUTE)
FROM UNNEST(GENERATE_ARRAY(1, 6000)) AS id;
