import os
import logging
from pyspark.sql.functions import lit, current_timestamp

logger = logging.getLogger(__name__)

def ingest_data(spark, raw_path, bronze_path):

    files = ["olist_customers_dataset.csv",
              "olist_geolocation_dataset.csv", 
              "olist_order_items_dataset.csv",
              "olist_order_payments_dataset.csv",
              "olist_order_reviews_dataset.csv",
              "olist_orders_dataset.csv",
              "olist_products_dataset.csv",
              "olist_sellers_dataset.csv",
              "product_category_name_translation.csv"
              ]
    #Todo : have to define schema for reading csv files.
    for file in files:
        try:
            logger.info(f"Reading file: {file}")

            df = spark.read.options(header= True, inferSchema= True).csv(str(raw_path/file))
            df = df.withColumn("ingested_at", current_timestamp())\
                    .withColumn("source_file", lit(file))
            row_count = df.count()
            df.write.mode("overwrite").parquet(str(bronze_path/file.split('.')[0]))

            logger.info(f"successfully ingested {row_count} rows from {file} written to {str(bronze_path/file.split('.')[0])}")

        except Exception as e:
            logger.error(f"Error: {e}")
    return None


        

