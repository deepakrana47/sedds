import sys
from pyspark.sql import *
from lib.logger import Log4J

if __name__ == "__main__":
    spark = SparkSession.builder \
            .appName("SDD") \
            .master("local[3]") \
            .getOrCreate()

    logger = Log4J(spark)

    logger.info("Start SDD")
    logger.info("Finish SDD")
    # spark.stop()