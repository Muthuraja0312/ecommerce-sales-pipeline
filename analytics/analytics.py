import duckdb

conn = duckdb.connect()

#Daily revenue 
print("\n=== DAILY REVENUE ===")
conn.sql("select order_date, total_revenue from read_parquet('./data/gold/daily_revenue/*.parquet') order by total_revenue desc limit 10").show()

#Average order value
print("\n=== AVERAGE ORDER VALUE ===")
conn.sql("select average_order_value from read_parquet('data/gold/average_order_value/*.parquet')").show()

#Revenue generated state wise
print("\n=== REVENUE GENERATED STATE WISE ===")
conn.sql("select customer_state, total_revenue from read_parquet('data/gold/revenue_per_state/*.parquet') order by total_revenue desc limit 10").show()

#Order volume per month
print("\n=== ORDER VOLUME PER MONTH ===")
conn.sql("select purchase_month, order_volume_per_month from read_parquet('data/gold/monthly_order_volume/*.parquet') order by purchase_month asc").show()

#Revenue per payment type
print("\n=== REVENUE PER PAYMENT TYPE ===")
conn.sql("select payment_type, revenue from read_parquet('data/gold/revenue_per_payment_type/*.parquet') order by revenue desc").show()

#Top 10 product category
print("\n=== TOP 10 PRODUCT CATEGORY ===")
conn.sql("select product_category_name_english as product_category, total_revenue_per_category from read_parquet('data/gold/top_product_category/*.parquet') order by total_revenue_per_category desc limit 10").show()