import duckdb
conn = duckdb.connect()

# print('=== NULL COUNTS ===')
# conn.sql("SELECT COUNT(*) - COUNT(order_id) as null_order_ids,COUNT(*) - COUNT(order_approved_at) as null_approved,COUNT(*) - COUNT(order_delivered_customer_date) as null_delivered,COUNT(*) - COUNT(order_status) as null_status FROM read_parquet('data/bronze/olist_orders_dataset/*.parquet')").show()

# print('=== DISTINCT STATUS VALUES ===')
# conn.sql("SELECT order_status, COUNT(*) as count FROM read_parquet('data/bronze/olist_orders_dataset/*.parquet') GROUP BY order_status ORDER BY count DESC").show()

# print('=== DUPLICATE ORDER IDS ===')
# conn.sql("SELECT COUNT(*) - COUNT(DISTINCT order_id) as duplicate_order_ids FROM read_parquet('data/bronze/olist_orders_dataset/*.parquet') ").show()

# conn.sql("SELECT  order_status, COUNT(*) as total, COUNT(*) - COUNT(order_delivered_customer_date) as null_delivery_date FROM read_parquet('data/bronze/olist_orders_dataset/*.parquet') GROUP BY order_status ORDER BY null_delivery_date DESC ").show()

# conn.sql("SELECT COUNT(*) as total_record_count from read_parquet('data/bronze/olist_orders_dataset/*.parquet') ").show()

# conn.sql(" SELECT  order_purchase_timestamp, order_approved_at, order_delivered_customer_date FROM read_parquet('data/bronze/olist_orders_dataset/*.parquet') WHERE order_delivered_customer_date IS NOT NULL LIMIT 5").show()


print('=== ORDERS SCHEMA ===')
conn.sql("DESCRIBE SELECT * FROM read_parquet('data/bronze/olist_orders_dataset/*.parquet')").show()
print('=== CUSTOMERS SCHEMA ===')
conn.sql("DESCRIBE SELECT * FROM read_parquet('data/bronze/olist_customers_dataset/*.parquet')").show()
print('=== PAYMENTS SCHEMA ===')
conn.sql("DESCRIBE SELECT * FROM read_parquet('data/bronze/olist_order_payments_dataset/*.parquet')").show()
print('=== ORDER ITEMS SCHEMA ===')
conn.sql("DESCRIBE SELECT * FROM read_parquet('data/bronze/olist_order_items_dataset/*.parquet')").show()
print("===PRODUCT SCHEMA===")
conn.sql("DESCRIBE SELECT * FROM read_parquet('data/bronze/olist_products_dataset/*.parquet')").show()

# conn.sql("SELECT COUNT(DISTINCT order_id) as orders_count FROM read_parquet('data/silver/cleaned_orders/*.parquet')").show()

# conn.sql("SELECT COUNT(DISTINCT order_id) as payment_orders_count FROM read_parquet('data/bronze/olist_order_payments_dataset/*.parquet')").show()

# conn.sql("SELECT order_id, COUNT(*) as payment_rows FROM read_parquet('data/bronze/olist_order_payments_dataset/*.parquet') GROUP BY order_id HAVING COUNT(*) > 1 ORDER BY payment_rows DESC LIMIT 5").show()

# conn.sql("SELECT COUNT(DISTINCT customer_id) as unique_customer_id, COUNT(DISTINCT customer_unique_id) as unique_customer_unique_id FROM read_parquet('data/bronze/olist_customers_dataset/*.parquet')").show()

# conn.sql("\
# SELECT \
# o.order_id,\
# SUM(p.payment_value) as total_payment,\
# SUM(i.price + i.freight_value) as total_items \
# FROM read_parquet('data/bronze/olist_orders_dataset/*.parquet') o \
# LEFT JOIN read_parquet('data/bronze/olist_order_payments_dataset/*.parquet') p \
# ON o.order_id = p.order_id \
# LEFT JOIN read_parquet('data/bronze/olist_order_items_dataset/*.parquet') i \
# ON o.order_id = i.order_id \
# GROUP BY o.order_id \
# LIMIT 5").show()

conn.sql("select * from read_parquet('data/bronze/olist_order_payments_dataset/*.parquet') limit 5").show()
# conn.sql("select order_id, sum(payment_value) as total_payment from read_parquet('data/bronze/olist_order_payments_dataset/*.parquet') group by order_id").show()

conn.sql("select count(*) as total_count, count(order_id) as order_count from read_parquet('data/bronze/olist_orders_dataset/*.parquet')").show()
conn.sql("select count(*) as total_count, count(order_id) as order_count from read_parquet('data/bronze/olist_order_items_dataset/*.parquet')").show()

conn.sql("select sum(total_count) from(select count(*) as total_count, count(order_id) as order_count from read_parquet('data/bronze/olist_order_items_dataset/*.parquet') group by order_id having count('order_id')>1)").show()
