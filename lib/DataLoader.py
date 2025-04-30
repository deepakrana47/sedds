from pyspark.sql.types import DateType, StructField, StructType, IntegerType, StringType

from . import ConfigLoader

"""
Return the account schema
"""
def get_account_schema():
    return StructType([
        StructField("load_date", DateType(), True),
        StructField("active_ind", IntegerType(), True),
        StructField("account_id", StringType(), True),
        StructField("source_sys", StringType(), True),
        StructField("account_start_date", DateType(), True),
        StructField("legal_title_1", StringType(), True),
        StructField("legal_title_2", StringType(), True),
        StructField("tax_id_type", StringType(), True),
        StructField("tax_id", StringType(), True),
        StructField("branch_code", StringType(), True),
        StructField("country", StringType(), True)
    ])

"""
Return the party schema
"""
def get_party_schema():
    return StructType([
        StructField("load_date", DateType(), True),
        StructField("account_id", StringType(), True),
        StructField("party_id", StringType(), True),
        StructField("relation_type", StringType(), True),
        StructField("relation_start_date", DateType(), True)
    ])

"""
Return the party schema
"""
def get_party_address_schema():
    return StructType([
        StructField("load_date", DateType(), True),
        StructField("party_id", StringType(), True),
        StructField("address_line_1", StringType(), True),
        StructField("address_line_2", StringType(), True),
        StructField("city", DateType(), True),
        StructField("postal_code", StringType(), True),
        StructField("country_of_address", StringType(), True),
        StructField("address_start_date", DateType(), True)
    ])

"""
For QA and PROD it read account data from hive table while for local read csv from test-data folder
Return the account dataframe
"""
def read_accounts(spark, env, enable_hive, hive_db):
    runtime_filter = ConfigLoader.get_data_filter(env, "account.filter")
    if enable_hive:
        return spark.sql("select * from " + hive_db + ".accounts").where(runtime_filter)
    else:
        return spark.read \
            .format("csv") \
            .option("header", "true") \
            .schema(get_account_schema()) \
            .load("test-data/accounts/") \
            .where(runtime_filter)

"""
For QA and PROD it read party data from hive table while for local read csv from test-data folder
Return the account dataframe
"""
def read_parties(spark, env, enable_hive, hive_db):
    runtime_filter = ConfigLoader.get_data_filter(env, "party.filter")
    if enable_hive:
        return spark.sql("select * from " + hive_db + ".party").where(runtime_filter)
    else:
        return spark.read \
            .format("csv") \
            .option("header", "true") \
            .schema(get_party_schema()) \
            .load("test-data/parties/") \
            .where(runtime_filter)

"""
For QA and PROD it read party address data from hive table while for local read csv from test-data folder
Return the account dataframe
"""
def read_party_address(spark, env, enable_hive, hive_db):
    runtime_filter = ConfigLoader.get_data_filter(env, "address.filter")
    if enable_hive:
        return spark.sql("select * from " + hive_db + ".party_address").where(runtime_filter)
    else:
        return spark.read \
            .format("csv") \
            .option("header", "true") \
            .schema(get_party_address_schema()) \
            .load("test-data/party_address/") \
            .where(runtime_filter)