import duckdb
conn = duckdb.connect()

print('=== NULL COUNTS ===')
conn.sql("SELECT COUNT(*) - COUNT(order_id) as null_order_ids,COUNT(*) - COUNT(order_approved_at) as null_approved,COUNT(*) - COUNT(order_delivered_customer_date) as null_delivered,COUNT(*) - COUNT(order_status) as null_status FROM read_parquet('data/bronze/olist_orders_dataset/*.parquet')").show()

print('=== DISTINCT STATUS VALUES ===')
conn.sql("SELECT order_status, COUNT(*) as count FROM read_parquet('data/bronze/olist_orders_dataset/*.parquet') GROUP BY order_status ORDER BY count DESC").show()

print('=== DUPLICATE ORDER IDS ===')
conn.sql("SELECT COUNT(*) - COUNT(DISTINCT order_id) as duplicate_order_ids FROM read_parquet('data/bronze/olist_orders_dataset/*.parquet') ").show()

conn.sql("SELECT  order_status, COUNT(*) as total, COUNT(*) - COUNT(order_delivered_customer_date) as null_delivery_date FROM read_parquet('data/bronze/olist_orders_dataset/*.parquet') GROUP BY order_status ORDER BY null_delivery_date DESC ").show()

conn.sql("SELECT COUNT(*) as total_record_count from read_parquet('data/bronze/olist_orders_dataset/*.parquet') ").show()

conn.sql(" SELECT  order_purchase_timestamp, order_approved_at, order_delivered_customer_date FROM read_parquet('data/bronze/olist_orders_dataset/*.parquet') WHERE order_delivered_customer_date IS NOT NULL LIMIT 5").show()