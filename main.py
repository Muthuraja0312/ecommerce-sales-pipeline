from pathlib import Path
import logging
import argparse

base_dir = Path(__file__).resolve().parent
log_file_path = base_dir/"logs"/"ecom.log"
rawdata_file_path = base_dir/"data"/"raw"
bronze_file_path = base_dir/"data"/"bronze"
silver_file_path = base_dir/"data"/"silver"
gold_file_path = base_dir/"data"/"gold"
quarantine_file_path = base_dir/"data"/"quarantine"

logging.basicConfig(filename=log_file_path,
                    filemode="a",
                    format="{asctime} - {name} - {message}",
                    style="{",
                    level=logging.INFO
                    )
logger = logging.getLogger(__name__)

from src.utils.spark_session import get_spark_session
from src.ingestion.ingest_raw import ingest_data
from src.transformation.clean_orders import clean_orders_data
from src.transformation.enrich_orders import enrich_order
from src.aggregation.gold_metrics import calculate_daily_revenue, calculate_revenue_by_state, calculate_avg_ordervalue, calculate_revenue_per_payment_type, calculate_monthly_order_volume, calculate_top_products_categories

if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(description="a parser that collect the stages to run in the pipeline")
    parser.add_argument("--stage", type=str, default="all", help="give which stage to run bronze, silver, gold or all")
    args = parser.parse_args()
    stage = args.stage

    try:
        spark = get_spark_session(appname="ecom_pipeline")
        logger.info("spark session started")

        if stage not in ["all", "bronze", "silver", "gold"]:
            logger.error(f"invalid stage value in argument. stage value: {stage}")
            raise Exception("Invalid stage value")
        
        if stage == "all" or stage == "bronze":
            ingest_data(spark, rawdata_file_path, bronze_file_path)
            logger.info("ingestion done")

        if stage == "all" or stage == "silver":
            logger.info("order data cleaning started")
            parquet_files = list(bronze_file_path.iterdir())
            if len(parquet_files) != 0:
                clean_orders_data(spark, bronze_file_path, silver_file_path,quarantine_file_path)
                logger.info("order data cleaning done")

                logger.info("enrich order started")
                enrich_order(spark, bronze_file_path, silver_file_path)
                logger.info("enrich order done")

        if stage == "all" or stage == "gold":
            logger.info("aggregation started")
            parquet_files = list(silver_file_path.iterdir())
            if len(parquet_files) != 0:
                orders_df = spark.read.parquet(str(silver_file_path/"enriched_orders"))
                calculate_daily_revenue(orders_df, gold_file_path)
                calculate_revenue_by_state(orders_df, gold_file_path)
                calculate_avg_ordervalue(orders_df, gold_file_path)
                calculate_revenue_per_payment_type(spark, bronze_file_path, gold_file_path)
                calculate_monthly_order_volume(orders_df, gold_file_path)
                calculate_top_products_categories(spark, bronze_file_path, gold_file_path)
                logger.info("aggregation done")
            
    except Exception as e:
        logger.error(f"Error : {e}")

    finally:
        if 'spark' in locals():
            spark.stop()
            logger.info("spark session stopped")