import logging
import pyspark.sql.functions as sf
from pyspark.sql import SparkSession
import botocore 
import botocore.session 
import json
from aws_secretsmanager_caching import SecretCache, SecretCacheConfig 


logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)

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


def get_clean_df(spark):
   
    df = spark.read.json("s3a://dataminded-academy-capstone-resources/raw/open_aq/")

    clean = (
        df.select(
            [
                sf.col(colname)
                if df.schema[colname].dataType.typeName() != "struct"
                else sf.col(f"{colname}.*")
                for colname in df.columns
            ]
        )
        .withColumn("utc", sf.to_date(sf.col("utc")))
        .withColumn("local", sf.to_date(sf.col("local")))
    )

    return clean

def get_snowflake_credentials():
    client = botocore.session.get_session().create_client('secretsmanager')
    cache_config = SecretCacheConfig()
    cache = SecretCache( config = cache_config, client = client)
    secret = cache.get_secret_string('snowflake/capstone/login')
    return json.loads(secret)


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
    "sfSchema": "ADRIANA",
    "sfWarehouse": credentials["WAREHOUSE"]}
      
    df.write.format(SNOWFLAKE_SOURCE_NAME) \
                .options(**sfOptions) \
                .option("dbtable", "weather") \
                .mode("overwrite").save()


    