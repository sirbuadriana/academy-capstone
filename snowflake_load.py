import logging

import pyspark.sql.functions as sf
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)


from snowflake import get_snowflake_credentials
from ingest import get_clean_df


SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

def get_spark_session(name: str = None) -> SparkSession:
    return (
        SparkSession.builder.config(
            "spark.jars.packages",
            ",".join(
                [
                    "org.apache.hadoop:hadoop-aws:3.3.1",
                    "net.snowflake:spark-snowflake_2.12:2.9.0-spark_3.1",
                    "net.snowflake:snowflake-jdbc:3.13.3",
                ]
            ),
        )
        .config(
            "fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
        )
        .appName(name)
        .getOrCreate()
    )



if __name__ == "__main__":
    credentials = get_snowflake_credentials()
    spark=get_spark_session("snowflake-connect")
    df = get_clean_df(spark)

    sfOptions = {
    "sfURL": credentials["URL"],
    "sfAccount": "yw41113",
    "sfUser": credentials["USER_NAME"],
    "sfPassword": credentials["PASSWORD"],
    "sfDatabase":  credentials["DATABASE"],
    "sfSchema": "ADRIANA_DM",
    "sfWarehouse": credentials["WAREHOUSE"]}

      
    df.write.format(SNOWFLAKE_SOURCE_NAME) \
                .options(**sfOptions) \
                .option("dbtable", "weather") \
                .mode("append").save()
        