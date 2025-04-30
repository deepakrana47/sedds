import sys, uuid
from pyspark.streaming.kinesis import KinesisUtils, InitialPositionInStream

from lib.logger import Log4J
from lib import ConfigLoader, DataLoader, Transformation, Utils

if __name__ == "__main__":

    if len(sys.argv) < 3:
        print("Usage: python3 main.py {local qa prod} {load_date} : some argument are missing")

    job_env = sys.argv[1].upper()
    load_date = sys.argv[2]
    job_run_id = uuid.uuid4()

    print(f"Initialize SDD :: env: {job_env} date: {load_date} job id: {job_run_id}")

    # loading env configuration
    print("Environment config is loaded")
    config = ConfigLoader.get_config(job_env)

    # load spark session
    print("Spark session is loaded")
    spark = Utils.get_spark_session(job_env)
    spark.sparkContext.setLogLevel("WARN")
    logger = Log4J(spark)

    # get hive database
    enable_hive = True if config["enable.hive"] == "true" else False
    hive_db = config["hive.database"]

    logger.info("Transformation start")

    # load account dataframe
    account_df = DataLoader.read_accounts(spark, job_env, enable_hive, hive_db)
    party_df = DataLoader.read_parties(spark, job_env, enable_hive, hive_db)
    address_df = DataLoader.read_party_address(spark, job_env, enable_hive, hive_db)

    # transform party address
    df1 = Transformation.process_address(address_df)

    # transform party data
    df2 = Transformation.process_parties(party_df)

    # transform account data
    df3 = Transformation.process_accounts(account_df)

    # and aggregate party based on address
    t1_df = Transformation.aggregate_account_party_address(df2, df1)

    # generate the final result
    final_df = Transformation.add_header(df3, t1_df)

    # print(final_df.show(2))
    # send data to AWS MSK
    api_key = config["kafka.api_key"]
    api_secret = config["kafka.api_secret"]
    final_df.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", config["kafka.bootstrap.servers"]) \
        .option("topic", config["kafka.topic"]) \
        .option("kafka.security.protocol", config["kafka.security.protocol"]) \
        .option("kafka.sasl.jaas.config", config["kafka.sasl.jaas.config"].format(api_key, api_secret)) \
        .option("kafka.sasl.mechanism", config["kafka.sasl.mechanism"]) \
        .option("kafka.client.dns.lookup", config["kafka.client.dns.lookup"]) \
        .save()

    # stop pyspark
    spark.stop()