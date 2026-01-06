import logging
import boto3
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from glue_etl_pipeline.glue_config import USER_MYSQL_URL,ORDER_MYSQL_URL,PRODUCT_MYSQL_URL,MYSQL_PROPERTIES


def get_glue_logger():
    """
    Returns a logger configured for AWS Glue.
    """
    logger = logging.getLogger("glue_etl_pipeline")
    logger.setLevel(logging.INFO)
    return logger

def read_from_rds(spark: SparkSession, database_url, tablename:str) -> DataFrame:
    """
    Reads data from an Database .
    """
    df=spark.read.jdbc(url=database_url, table=tablename, properties=MYSQL_PROPERTIES)
    df.createOrReplaceTempView(tablename)
    print("Reading from RDS Table -- " +tablename+ "  count -- " + str(df.count()))
    return df


def read_from_dynamodb(glueContext: GlueContext, tableName:str,region) -> DataFrame:
    """
    Reads data from an Database .
    """
    print("Reading from dynamodb Table --   "+tableName +"   region -- " + region )
    dyf = glueContext.create_dynamic_frame.from_options(
        connection_type="dynamodb",
        connection_options={
            "dynamodb.input.tableName": tableName,
            "dynamodb.region": region
        }
    )
    df = dyf.toDF()
    print("Reading from dynamodb Table -- " +tableName+ "  count -- " + str(df.count()))
    df.printSchema()
    return df

def read_from_s3(glue_context: GlueContext, s3_path: str, format="csv", options=None) -> DataFrame:
    """
    Reads data from an S3 location.
    """
    options = options or {"header": "true"}
    return glue_context.read.format(format).options(**options).load(s3_path)

def write_to_s3(df: DataFrame, s3_path: str, format="parquet", mode="overwrite"):
    """
    Writes a Spark DataFrame to an S3 location.
    """
    print(f"Write data to S3 Started: {s3_path}")
    df.show(10)
    print(df.count())
    df.write.mode(mode).format(format).save(s3_path)
    print(f"Write data to S3 Completed: {s3_path}")