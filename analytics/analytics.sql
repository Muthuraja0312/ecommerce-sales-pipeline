-- =============================================================
-- E-Commerce Sales Pipeline — Analytics Queries
-- Author: Muthuraja Muruganantham
-- Dataset: Olist Brazilian E-Commerce
-- Analytics layer reads from gold parquet files via DuckDB
-- Run: python3 queries/run_analytics.py
-- =============================================================


--Daily revenue
select order_date, total_revenue 
from read_parquet('./data/gold/daily_revenue/*.parquet') 
order by total_revenue 
desc limit 10;

--Average order value
select average_order_value 
from read_parquet('data/gold/average_order_value/*.parquet');

--Revenue generated state wise
select customer_state, total_revenue 
from read_parquet('data/gold/revenue_per_state/*.parquet') 
order by total_revenue 
desc limit 10;

--Order volume per month
select purchase_month, order_volume_per_month 
from read_parquet('data/gold/monthly_order_volume/*.parquet') 
order by purchase_month asc;

--Revenue per payment type
select payment_type, revenue 
from read_parquet('data/gold/revenue_per_payment_type/*.parquet') 
order by revenue desc;

--top 10 product category
select product_category_name_english as product_category, total_revenue_per_category 
from read_parquet('data/gold/top_product_category/*.parquet') 
order by total_revenue_per_category 
desc limit 10;