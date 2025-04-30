from pyspark.sql import SparkSession
from lib.ConfigLoader import get_spark_config

def get_spark_session(env):
    spark_session = None

    spark_config = get_spark_config(env)
    if env == "LOCAL":
        spark_session = SparkSession.builder \
            .config(conf=spark_config) \
            .config('spark.sql.autoBroadcastJoinThreshold',-1) \
            .config('spark.sql.adaptive.enabled',"false") \
            .config('spark.driver.extraJavaOptions',
                    '-Dlog4j.configuration=file:log4j.properties -Dspark.yarn.app.container.log.dir=app-logs -Dlogfile.name=spark-logs') \
            .master('local[2]') \
            .enableHiveSupport() \
            .getOrCreate()
    elif env == "QA":
        spark_session = SparkSession.builder \
            .config(conf=spark_config) \
            .enableHiveSupport() \
            .getOrCreate()
    elif env == "PROD":
        spark_session = SparkSession.builder \
            .config(conf=spark_config) \
            .enableHiveSupport() \
            .getOrCreate()
    else:
        print("Environment is not defined")
    return spark_session