import logging
from pyspark.sql.functions import to_date, col, sum as spark_sum, round, avg, count as spark_count, date_format, lit, coalesce

logger = logging.getLogger(__name__)

def calculate_daily_revenue(order_df, gold_path):
    
    try:
        logger.info(f"{order_df.count()} rows got from function call")

        daily_revenue_df = order_df.groupBy(to_date(col("order_purchase_timestamp")).alias("order_date")).agg(round(spark_sum(col("total_payment_value")), 2).alias("total_revenue"))

        daily_revenue_df.write.mode("overwrite").parquet(str(gold_path/"daily_revenue"))
        logger.info(f"{daily_revenue_df.count()} rows written to {str(gold_path/"daily_revenue")}")

    except Exception as e:
        logger.error(f"error while calculating daily revenue : {e}")

    return None


def calculate_revenue_by_state(order_df, gold_path):
    
    try:
        logger.info(f"{order_df.count()} rows got from function call")

        revenue_by_state_df = order_df.groupBy("customer_state").agg(round(spark_sum("total_payment_value"), 2).alias("total_revenue"))
        total_count = revenue_by_state_df.count()

        revenue_by_state_df.write.mode("overwrite").parquet(str(gold_path/"revenue_per_state"))
        logger.info(f"{total_count} rows written to {str(gold_path/"revenue_per_state")}")

    except Exception as e:
        logger.error(f" error while calculating revenue per state: {e}")


def calculate_avg_ordervalue(order_df, gold_path):
    try:
        logger.info(f"{order_df.count()} rows got from function call")
        avg_ordervalue_df = order_df.agg(round(avg("total_payment_value"), 2).alias("average_order_value"))

        avg_ordervalue_df.write.mode("overwrite").parquet(str(gold_path/"average_order_value"))
        logger.info(f"{avg_ordervalue_df.count()} row written to  {str(gold_path/"average_order_value")}")

    except Exception as e:
        logger.error(f"error while calculating avarage order value : {e}")


def calculate_revenue_per_payment_type(spark, bronze_path, gold_path):

    try:
        logger.info("calculate revenue per payment type started.")
        payment_df = spark.read.parquet(str(bronze_path/"olist_order_payments_dataset"))
        payment_df_count = payment_df.count()
        logger.info(f"{payment_df_count} rows read from {str(bronze_path/"olist_order_payments_dataset")}")

        payment_df = payment_df.groupBy(col("payment_type")).agg(round(spark_sum(col("payment_value")),2).alias("revenue"))
        payment_df_count = payment_df.count()
        payment_df.write.mode("overwrite").parquet(str(gold_path/"revenue_per_payment_type"))
        logger.info(f"{payment_df_count} rows written to {str(gold_path/"revenue_per_payment_type")}")

    except Exception as e:
        logger.error(f"error while calculating value per paymment type: {e}")


def calculate_monthly_order_volume(order_df, gold_path):
    try:
        logger.info(f"{order_df.count()} rows got from function call")
        monthly_order_volume = order_df.groupBy(date_format(col("order_purchase_timestamp"), "yyyy-MM").alias("purchase_month")).agg(spark_count(col("order_id")).alias("order_volume_per_month")).sort(col("purchase_month"))
        monthly_order_volume_count = monthly_order_volume.count()
        monthly_order_volume.write.mode("overwrite").parquet(str(gold_path/"monthly_order_volume"))
        logger.info(f"{monthly_order_volume_count} rows written to {str(gold_path/"monthly_order_volume")}")

    except Exception as e:
        logger.error(f"error while doing monthly order volume: {e}")

    return None


def calculate_top_products_categories(spark, bronze_path, gold_path):

    try:
        order_items_df = spark.read.parquet(str(bronze_path/"olist_order_items_dataset"))
        logger.info(f"{order_items_df.count()} rows read from {str(bronze_path/"olist_order_items_dataset")}")

        products_df = spark.read.parquet(str(bronze_path/"olist_products_dataset"))
        logger.info(f"{products_df.count()} rows read from {str(bronze_path/"olist_products_dataset")}")

        product_category_df = spark.read.parquet(str(bronze_path/"product_category_name_translation"))
        logger.info(f"{product_category_df.count()} rows read from {str(bronze_path/"product_category_name_translation")}")

        final_df = order_items_df.alias("o").join(products_df.alias("p"),col("o.product_id") == col("p.product_id"), "left").join(product_category_df.alias("c"), col("p.product_category_name") == col("c.product_category_name"), "left")\
            .select(col("o.order_id").alias("order_id"), col("p.product_id").alias("product_id"), col("o.price").alias("price"), col("o.freight_value").alias("freight_value"), col("p.product_category_name").alias("product_category_name"), col("c.product_category_name_english").alias("product_category_name_english"))
        final_df = final_df.groupBy("product_category_name_english").agg(round(spark_sum(coalesce(col("price"), lit(0))+coalesce(col("freight_value"), lit(0))),2).alias("total_revenue_per_category"))
        final_df.write.mode("overwrite").parquet(str(gold_path/"top_product_category"))
        logger.info(f"{final_df.count()} rows written to {str(gold_path/"top_product_category")}")

    except Exception as e:
        logger.error(f"error while joining {e}")
    return None