import sys
import boto3
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

from glue_etl_pipeline.transformations import transform_data
from glue_etl_pipeline.utils import get_glue_logger

# Parse job arguments
args = getResolvedOptions(sys.argv, ["JOB_NAME", "S3_SOURCE_PATH", "S3_TARGET_PATH"])

# Initialize Spark and Glue Context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Initialize Logger
logger = get_glue_logger()

def run_etl():
    try:
        logger.info("Starting Glue ETL Job")

        # Load data from S3
        logger.info(f"Reading data from {args['S3_SOURCE_PATH']}")
        df = spark.read.option("header", True).csv(args["S3_SOURCE_PATH"])

        # Transform Data
        df_transformed = transform_data(df)

        # Write transformed data to S3
        logger.info(f"Writing transformed data to {args['S3_TARGET_PATH']}")
        df_transformed.write.mode("overwrite").parquet(args["S3_TARGET_PATH"])

        logger.info("ETL Job Completed Successfully")

        job.commit()
    except Exception as e:
        logger.error(f"ETL Job Failed: {str(e)}")
        raise

if __name__ == "__main__":
    run_etl()
