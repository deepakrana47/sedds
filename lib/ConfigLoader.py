import configparser
from pyspark import SparkConf

"""
Input: Takes the environment as input
Function: Get the environment specific configuration
Output: Return the input env configuration 
"""
def get_config(env):
    config_file = "conf/sdd.conf"
    config = configparser.ConfigParser()
    config.read(config_file)
    return {k:v for k, v in config.items(env)}

"""
Input: Takes the environment as input 
Function: Create a spark conf with environment specific configuration 
Output: Return the spark configuration for the input environment 
"""
def get_spark_config(env):
    spark_config = SparkConf()
    config_file = "conf/spark.conf"
    config = configparser.ConfigParser()
    config.read(config_file)
    for k, v in config.items(env):
        spark_config.set(k, v)
    return spark_config

"""
Input: Takes the environment as input and data filter keyword
Function: Its a condition to select specific records e.g. active records, last 1 year, etc. 
Output: Return the spark configuration for the input environment 
"""
def get_data_filter(env, data_filter):
    conf = get_config(env)
    return "true" if conf[data_filter] == "" else conf[data_filter]