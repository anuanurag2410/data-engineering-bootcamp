-- BigQuery DDL for Omni-Commerce ODS (50 tables)
-- Project: `datamigrationproject-475415`
-- Dataset: `ods`
-- Generated: 2025-10-17T15:39:45.555453Z
-- Description: Create dataset and all 50 ODS tables with partitions and clustering.
-- Adjust PARTITION/CLUSTER hints as per data volume and query patterns.

-- Step 1: Create the dataset (if not exists)
CREATE SCHEMA IF NOT EXISTS `datamigrationproject-475415.ods`
OPTIONS(
  location = 'US',
  description = 'ODS Layer for OmniCommerce Migration Project (Source for Databricks)'
);

-- Step 2: Create tables
CREATE TABLE IF NOT EXISTS `datamigrationproject-475415.ods.ods_customers` (
  customer_id INT64,
  first_name STRING,
  last_name STRING,
  email STRING,
  phone STRING,
  birth_date DATE,
  gender STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS `datamigrationproject-475415.ods.ods_customer_addresses` (
  address_id INT64,
  customer_id INT64,
  address_type STRING,
  line1 STRING,
  line2 STRING,
  city STRING,
  state STRING,
  postal_code STRING,
  country STRING,
  is_primary BOOL,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS `datamigrationproject-475415.ods.ods_customer_preferences` (
  pref_id INT64,
  customer_id INT64,
  marketing_opt_in BOOL,
  preferred_channel STRING,
  language STRING,
  currency STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS `datamigrationproject-475415.ods.ods_loyalty_accounts` (
  loyalty_id INT64,
  customer_id INT64,
  program_tier STRING,
  points_balance INT64,
  enrolled_at TIMESTAMP,
  updated_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS `datamigrationproject-475415.ods.ods_loyalty_transactions` (
  loyalty_txn_id INT64,
  loyalty_id INT64,
  txn_type STRING,
  points INT64,
  txn_ts TIMESTAMP,
  order_id INT64,
  notes STRING
)
PARTITION BY DATE(txn_ts)
CLUSTER BY loyalty_id, order_id;

CREATE TABLE IF NOT EXISTS `datamigrationproject-475415.ods.ods_products` (
  product_id INT64,
  sku STRING,
  product_name STRING,
  brand STRING,
  category_id INT64,
  sub_category_id INT64,
  status STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS `datamigrationproject-475415.ods.ods_skus` (
  sku_id INT64,
  product_id INT64,
  sku STRING,
  upc STRING,
  model STRING,
  color STRING,
  size STRING,
  weight FLOAT64,
  active BOOL,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS `datamigrationproject-475415.ods.ods_categories` (
  category_id INT64,
  category_name STRING,
  parent_category_id INT64,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS `datamigrationproject-475415.ods.ods_sub_categories` (
  sub_category_id INT64,
  category_id INT64,
  sub_category_name STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS `datamigrationproject-475415.ods.ods_price_lists` (
  price_list_id INT64,
  name STRING,
  currency STRING,
  valid_from DATE,
  valid_to DATE,
  is_default BOOL,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS `datamigrationproject-475415.ods.ods_prices` (
  price_id INT64,
  price_list_id INT64,
  sku_id INT64,
  base_price NUMERIC,
  discount NUMERIC,
  final_price NUMERIC,
  valid_from DATE,
  valid_to DATE,
  updated_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS `datamigrationproject-475415.ods.ods_suppliers` (
  supplier_id INT64,
  supplier_name STRING,
  contact_email STRING,
  phone STRING,
  country STRING,
  active BOOL,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS `datamigrationproject-475415.ods.ods_warehouses` (
  warehouse_id INT64,
  warehouse_name STRING,
  location_city STRING,
  location_state STRING,
  country STRING,
  capacity INT64,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS `datamigrationproject-475415.ods.ods_inventory` (
  inventory_id INT64,
  sku_id INT64,
  warehouse_id INT64,
  on_hand INT64,
  allocated INT64,
  available INT64,
  safety_stock INT64,
  as_of_ts TIMESTAMP
)
PARTITION BY DATE(as_of_ts)
CLUSTER BY sku_id, warehouse_id;

CREATE TABLE IF NOT EXISTS `datamigrationproject-475415.ods.ods_stock_movements` (
  movement_id INT64,
  sku_id INT64,
  warehouse_id INT64,
  source_type STRING,
  source_id INT64,
  qty INT64,
  reason STRING,
  txn_ts TIMESTAMP
)
PARTITION BY DATE(txn_ts)
CLUSTER BY sku_id, warehouse_id;

CREATE TABLE IF NOT EXISTS `datamigrationproject-475415.ods.ods_purchase_orders` (
  po_id INT64,
  supplier_id INT64,
  warehouse_id INT64,
  po_number STRING,
  status STRING,
  order_date DATE,
  expected_date DATE,
  total_amount NUMERIC,
  currency STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
)
PARTITION BY order_date
CLUSTER BY supplier_id, warehouse_id;

CREATE TABLE IF NOT EXISTS `datamigrationproject-475415.ods.ods_purchase_order_items` (
  po_item_id INT64,
  po_id INT64,
  sku_id INT64,
  qty_ordered INT64,
  unit_cost NUMERIC,
  qty_received INT64,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
)
CLUSTER BY po_id, sku_id;

CREATE TABLE IF NOT EXISTS `datamigrationproject-475415.ods.ods_channels` (
  channel_id INT64,
  channel_name STRING,
  description STRING
);

CREATE TABLE IF NOT EXISTS `datamigrationproject-475415.ods.ods_stores` (
  store_id INT64,
  store_code STRING,
  store_name STRING,
  city STRING,
  state STRING,
  country STRING,
  opened_at TIMESTAMP,
  closed_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS `datamigrationproject-475415.ods.ods_carts` (
  cart_id INT64,
  customer_id INT64,
  channel_id INT64,
  created_at TIMESTAMP,
  abandoned_at TIMESTAMP
)
PARTITION BY DATE(created_at)
CLUSTER BY customer_id;

CREATE TABLE IF NOT EXISTS `datamigrationproject-475415.ods.ods_cart_items` (
  cart_item_id INT64,
  cart_id INT64,
  sku_id INT64,
  qty INT64,
  unit_price NUMERIC,
  added_at TIMESTAMP
)
PARTITION BY DATE(added_at)
CLUSTER BY cart_id, sku_id;

CREATE TABLE IF NOT EXISTS `datamigrationproject-475415.ods.ods_orders` (
  order_id INT64,
  customer_id INT64,
  channel_id INT64,
  store_id INT64,
  order_number STRING,
  order_ts TIMESTAMP,
  status STRING,
  currency STRING,
  subtotal STRING,
  tax NUMERIC,
  shipping NUMERIC,
  total_amount NUMERIC,
  payment_status STRING
)
PARTITION BY DATE(order_ts)
CLUSTER BY customer_id, store_id;

CREATE TABLE IF NOT EXISTS `datamigrationproject-475415.ods.ods_order_items` (
  order_item_id INT64,
  order_id INT64,
  sku_id INT64,
  qty INT64,
  unit_price NUMERIC,
  discount NUMERIC,
  line_amount NUMERIC,
  fulfillment_status STRING
);

CREATE TABLE IF NOT EXISTS `datamigrationproject-475415.ods.ods_payments` (
  payment_id INT64,
  order_id INT64,
  payment_method STRING,
  amount NUMERIC,
  provider STRING,
  auth_code STRING,
  payment_ts TIMESTAMP,
  status STRING
)
PARTITION BY DATE(payment_ts)
CLUSTER BY order_id;

CREATE TABLE IF NOT EXISTS `datamigrationproject-475415.ods.ods_refunds` (
  refund_id INT64,
  payment_id INT64,
  order_id INT64,
  amount NUMERIC,
  reason STRING,
  refund_ts TIMESTAMP,
  status STRING
)
PARTITION BY DATE(refund_ts)
CLUSTER BY order_id, payment_id;

CREATE TABLE IF NOT EXISTS `datamigrationproject-475415.ods.ods_chargebacks` (
  chargeback_id INT64,
  payment_id INT64,
  order_id INT64,
  amount NUMERIC,
  opened_ts TIMESTAMP,
  closed_ts TIMESTAMP,
  status STRING
)
PARTITION BY DATE(opened_ts)
CLUSTER BY order_id, payment_id;

CREATE TABLE IF NOT EXISTS `datamigrationproject-475415.ods.ods_shipments` (
  shipment_id INT64,
  order_id INT64,
  carrier STRING,
  service_level STRING,
  tracking_no STRING,
  shipped_ts TIMESTAMP,
  delivered_ts TIMESTAMP,
  status STRING,
  warehouse_id INT64
)
PARTITION BY DATE(shipped_ts)
CLUSTER BY order_id, warehouse_id;

CREATE TABLE IF NOT EXISTS `datamigrationproject-475415.ods.ods_shipment_items` (
  shipment_item_id INT64,
  shipment_id INT64,
  order_item_id INT64,
  qty_shipped INT64
)
CLUSTER BY shipment_id, order_item_id;

CREATE TABLE IF NOT EXISTS `datamigrationproject-475415.ods.ods_delivery_partners` (
  partner_id INT64,
  partner_name STRING,
  contact_email STRING,
  phone STRING,
  country STRING,
  active BOOL
);

CREATE TABLE IF NOT EXISTS `datamigrationproject-475415.ods.ods_delivery_routes` (
  route_id INT64,
  partner_id INT64,
  shipment_id INT64,
  route_seq INT64,
  hub STRING,
  arrival_ts TIMESTAMP,
  departure_ts TIMESTAMP
)
PARTITION BY DATE(arrival_ts)
CLUSTER BY partner_id, shipment_id;

CREATE TABLE IF NOT EXISTS `datamigrationproject-475415.ods.ods_delivery_events` (
  event_id INT64,
  shipment_id INT64,
  event_type STRING,
  event_ts TIMESTAMP,
  location STRING,
  notes STRING
)
PARTITION BY DATE(event_ts)
CLUSTER BY shipment_id;

CREATE TABLE IF NOT EXISTS `datamigrationproject-475415.ods.ods_returns` (
  return_id INT64,
  order_id INT64,
  customer_id INT64,
  initiated_ts TIMESTAMP,
  approved_ts TIMESTAMP,
  status STRING,
  refund_id INT64
)
PARTITION BY DATE(initiated_ts)
CLUSTER BY order_id, customer_id;

CREATE TABLE IF NOT EXISTS `datamigrationproject-475415.ods.ods_return_items` (
  return_item_id INT64,
  return_id INT64,
  order_item_id INT64,
  qty INT64,
  reason STRING,
  condition STRING,
  resolution STRING
)
CLUSTER BY return_id, order_item_id;

CREATE TABLE IF NOT EXISTS `datamigrationproject-475415.ods.ods_support_tickets` (
  ticket_id INT64,
  customer_id INT64,
  order_id INT64,
  opened_ts TIMESTAMP,
  closed_ts TIMESTAMP,
  priority STRING,
  status STRING,
  channel STRING
);

CREATE TABLE IF NOT EXISTS `datamigrationproject-475415.ods.ods_support_interactions` (
  interaction_id INT64,
  ticket_id INT64,
  agent_id INT64,
  interaction_ts TIMESTAMP,
  medium STRING,
  notes STRING
);

CREATE TABLE IF NOT EXISTS `datamigrationproject-475415.ods.ods_support_agents` (
  agent_id INT64,
  agent_name STRING,
  email STRING,
  team STRING,
  location STRING,
  active_since TIMESTAMP
);

CREATE TABLE IF NOT EXISTS `datamigrationproject-475415.ods.ods_marketing_campaigns` (
  campaign_id INT64,
  campaign_name STRING,
  channel_id INT64,
  start_date DATE,
  end_date DATE,
  budget NUMERIC,
  currency STRING,
  status STRING
);

CREATE TABLE IF NOT EXISTS `datamigrationproject-475415.ods.ods_marketing_adgroups` (
  adgroup_id INT64,
  campaign_id INT64,
  adgroup_name STRING,
  status STRING,
  created_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS `datamigrationproject-475415.ods.ods_marketing_ads` (
  ad_id INT64,
  adgroup_id INT64,
  ad_name STRING,
  format STRING,
  status STRING,
  created_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS `datamigrationproject-475415.ods.ods_ad_impressions` (
  impression_id INT64,
  ad_id INT64,
  impression_ts TIMESTAMP,
  device STRING,
  geo STRING,
  placement STRING
)
PARTITION BY DATE(impression_ts)
CLUSTER BY ad_id;

CREATE TABLE IF NOT EXISTS `datamigrationproject-475415.ods.ods_ad_clicks` (
  click_id INT64,
  ad_id INT64,
  click_ts TIMESTAMP,
  device STRING,
  geo STRING,
  placement STRING,
  utm_campaign STRING,
  utm_medium STRING,
  utm_source STRING
)
PARTITION BY DATE(click_ts)
CLUSTER BY ad_id;

CREATE TABLE IF NOT EXISTS `datamigrationproject-475415.ods.ods_web_sessions` (
  session_id INT64,
  customer_id INT64,
  session_start_ts TIMESTAMP,
  device STRING,
  geo STRING,
  source STRING,
  medium STRING,
  campaign STRING
)
PARTITION BY DATE(session_start_ts)
CLUSTER BY customer_id;

CREATE TABLE IF NOT EXISTS `datamigrationproject-475415.ods.ods_web_events` (
  event_id INT64,
  session_id INT64,
  event_name STRING,
  event_ts TIMESTAMP,
  page_url STRING,
  product_id INT64,
  cart_id INT64,
  order_id INT64
)
PARTITION BY DATE(event_ts)
CLUSTER BY session_id, order_id;

CREATE TABLE IF NOT EXISTS `datamigrationproject-475415.ods.ods_invoices` (
  invoice_id INT64,
  order_id INT64,
  invoice_no STRING,
  invoice_ts TIMESTAMP,
  amount_due NUMERIC,
  amount_paid NUMERIC,
  status STRING,
  currency STRING
)
PARTITION BY DATE(invoice_ts)
CLUSTER BY order_id;

CREATE TABLE IF NOT EXISTS `datamigrationproject-475415.ods.ods_invoice_lines` (
  invoice_line_id INT64,
  invoice_id INT64,
  sku_id INT64,
  qty INT64,
  unit_price NUMERIC,
  line_total NUMERIC
)
CLUSTER BY invoice_id, sku_id;

CREATE TABLE IF NOT EXISTS `datamigrationproject-475415.ods.ods_gl_accounts` (
  gl_account_id INT64,
  gl_code STRING,
  gl_name STRING,
  gl_type STRING
);

CREATE TABLE IF NOT EXISTS `datamigrationproject-475415.ods.ods_financial_postings` (
  posting_id INT64,
  gl_account_id INT64,
  order_id INT64,
  amount NUMERIC,
  posting_ts TIMESTAMP,
  dimension1 STRING,
  dimension2 STRING
)
PARTITION BY DATE(posting_ts)
CLUSTER BY gl_account_id, order_id;

CREATE TABLE IF NOT EXISTS `datamigrationproject-475415.ods.ods_promotions` (
  promo_id INT64,
  promo_code STRING,
  description STRING,
  start_date DATE,
  end_date DATE,
  discount_type STRING,
  discount_value NUMERIC,
  status STRING
);

CREATE TABLE IF NOT EXISTS `datamigrationproject-475415.ods.ods_promo_eligibility` (
  promo_elig_id INT64,
  promo_id INT64,
  customer_id INT64,
  eligible_from DATE,
  eligible_to DATE,
  used_flag BOOL
);

CREATE TABLE IF NOT EXISTS `datamigrationproject-475415.ods.ods_promo_redemptions` (
  promo_red_id INT64,
  promo_id INT64,
  order_id INT64,
  redeemed_ts TIMESTAMP,
  discount_amount NUMERIC
);

CREATE TABLE IF NOT EXISTS `datamigrationproject-475415.ods.ods_subscriptions` (
  subscription_id INT64,
  customer_id INT64,
  plan_id INT64,
  start_date DATE,
  end_date DATE,
  status STRING,
  auto_renew BOOL
);

CREATE TABLE IF NOT EXISTS `datamigrationproject-475415.ods.ods_plans` (
  plan_id INT64,
  plan_name STRING,
  price NUMERIC,
  billing_cycle STRING,
  currency STRING,
  status STRING
);

CREATE TABLE IF NOT EXISTS `datamigrationproject-475415.ods.ods_subscription_events` (
  sub_event_id INT64,
  subscription_id INT64,
  event_type STRING,
  event_ts TIMESTAMP,
  notes STRING
);

CREATE TABLE IF NOT EXISTS `datamigrationproject-475415.ods.ods_reviews` (
  review_id INT64,
  product_id INT64,
  customer_id INT64,
  rating INT64,
  review_text STRING,
  review_ts TIMESTAMP,
  helpful_votes INT64
);

CREATE TABLE IF NOT EXISTS `datamigrationproject-475415.ods.ods_qna` (
  qna_id INT64,
  product_id INT64,
  customer_id INT64,
  question STRING,
  answer STRING,
  asked_ts TIMESTAMP,
  answered_ts TIMESTAMP
);
