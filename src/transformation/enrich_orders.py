import logging
from pyspark.sql.functions import sum as spark_sum, col

logger = logging.getLogger(__name__)

def enrich_order(spark, bronze_path, silver_path):

    try:
        logger.info("started reading from silver and bronze layer for enrich order")

        cleaned_orders_df = spark.read.parquet(str(silver_path/"cleaned_orders"))
        logger.info(f"read {cleaned_orders_df.count()} rows from {str(silver_path/"cleaned_orders")}")

        customers_df = spark.read.parquet(str(bronze_path/"olist_customers_dataset")).select("customer_id", "customer_unique_id", "customer_state")
        logger.info(f"read {customers_df.count()} rows from {str(bronze_path/"olist_customers_dataset")}")

        aggregated_payment_df = spark.read.parquet(str(bronze_path/'olist_order_payments_dataset'))
        logger.info(f"read {aggregated_payment_df.count()} rows from {str(bronze_path/"olist_order_payments_dataset")}")
        
        aggregated_payment_df = aggregated_payment_df.groupBy("order_id").agg(spark_sum("payment_value").alias("total_payment_value"))
        logger.info(f"{aggregated_payment_df.count()} rows after aggregating payments")
        
        order_customer_df = cleaned_orders_df.alias("o").join(customers_df.alias("c"), col("o.customer_id") == col("c.customer_id"),"left").drop(col("o.customer_id"))
        logger.info(f"{order_customer_df.count()} rows after joining orders with customers")

        result = order_customer_df.alias("oc").join(aggregated_payment_df.alias("p"), col("oc.order_id") == col("p.order_id"), "left").drop(col("p.order_id"))
        
        final_df = result.select("order_id", "order_status", "customer_id", "customer_unique_id", "customer_state", "total_payment_value", "order_purchase_timestamp")
        enriched_count = final_df.count()

        final_df.write.mode("overwrite").parquet(str(silver_path/"enriched_orders"))
        logger.info(f"{enriched_count} rows written to {str(silver_path/"enriched_orders")}")

    except Exception as e:
        logger.error(f"error while doing operation: {e}")
