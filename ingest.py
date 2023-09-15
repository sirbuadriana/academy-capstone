import logging

import pyspark.sql.functions as sf
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)


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
    spark = get_spark_session("ingest")
    spark.sparkContext.setLogLevel("ERROR")

    logger.info("Reading data from S3...")
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

    clean.show()
    logger.info("Done!")
