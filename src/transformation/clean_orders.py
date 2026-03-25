import logging
from pyspark.sql.functions import col, to_timestamp, when


logger = logging.getLogger(__name__)


def clean_orders_data(spark, file_path, silver_path, quarantine_path):
    try:
        df = spark.read.parquet(str(file_path/"olist_orders_dataset"/"*.parquet"))
        total_count = df.count()
        logger.info(f"successfully read {total_count} rows from orders data")

        df = df.dropna(subset = ["order_id"])
        after_dropna_count = df.count()

        logger.info(f"rows dropped due to order id is null {total_count - after_dropna_count}")

        df = df.dropDuplicates(subset = ["order_id"])

        logger.info(f"rows dropped due to duplicate order id {after_dropna_count - df.count()}")

        df = (df.withColumn("order_purchase_timestamp", to_timestamp("order_purchase_timestamp", "yyyy-MM-dd HH:mm:ss"))
            .withColumn("order_approved_at", to_timestamp("order_approved_at", "yyyy-MM-dd HH:mm:ss"))
            .withColumn("order_delivered_carrier_date", to_timestamp("order_delivered_carrier_date", "yyyy-MM-dd HH:mm:ss"))
            .withColumn("order_delivered_customer_date", to_timestamp("order_delivered_customer_date", "yyyy-MM-dd HH:mm:ss"))
            .withColumn("order_estimated_delivery_date", to_timestamp("order_estimated_delivery_date", "yyyy-MM-dd HH:mm:ss")))
        
        df = (df.withColumn("rejection_reason", when(~col("order_status").isin("shipped", "canceled", "invoiced", "processing", "delivered", "created", "approved") | col("order_status").isNull(), "invalid_status")
                            .when((col("order_status") == "delivered") & (col("order_delivered_customer_date").isNull()), "delivered_missing_date").otherwise(None))
                )
        
        quarantine_df = df.filter(col("rejection_reason").isNotNull())
        logger.info(f"{quarantine_df.count()} rows wrote to quarantine due to invalid order_status")

        cleaned_df = df.filter(col("rejection_reason").isNull())
        cleaned_df = cleaned_df.drop("rejection_reason")

        cleaned_count = cleaned_df.count()
        quarantine_count = quarantine_df.count()

        cleaned_df.write.mode("overwrite").parquet(str(silver_path/"cleaned_orders"))
        quarantine_df.write.mode("overwrite").parquet(str(quarantine_path/"quarantine_orders"))

        logger.info(f"{cleaned_count} rows wrote to {str(silver_path/"cleaned_orders")}")
        logger.info(f"{quarantine_count} rows wrote to {str(quarantine_path/"quarantine_orders")}")
    except FileNotFoundError as fe:
        logger.error(f"Error while reading : {fe}")
    except Exception as e:
        logger.error(e)

    return None