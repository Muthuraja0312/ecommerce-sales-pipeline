from pyspark.sql import SparkSession
import logging


logger = logging.getLogger(__name__)



def get_spark_session(appname = "ecom_pipeline"):
    
    try:
        spark = (SparkSession.builder
                            .appName(appname)
                            .master("local[*]")
                            .config("spark.sql.shuffle.partitions", "8")
                            .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
                            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")

        if spark is not None:
            logger.info("spark session created successfully")
            logger.info(f"Spark version: {spark.version}, "
                            f"Spark app name: {spark.conf.get('spark.app.name')}, "
                            f"Number of cores: {spark.sparkContext.defaultParallelism}")
        return spark
    
    except Exception as e:
        logger.error(e)