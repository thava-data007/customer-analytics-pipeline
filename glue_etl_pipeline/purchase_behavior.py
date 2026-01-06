import sys
import boto3
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from transformations.customer_ranking import transform_top_customers_sql, transform_dataframe    
from glue_etl_pipeline.utils import get_glue_logger,read_from_rds,write_to_s3
from glue_etl_pipeline.glue_config import USER_MYSQL_URL,ORDER_MYSQL_URL,PRODUCT_MYSQL_URL

# Parse job arguments
args = getResolvedOptions(sys.argv, ["JOB_NAME", "S3_TARGET_PATH"])


# Initialize Spark and Glue Context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
s3_output_path =args['S3_TARGET_PATH'] +args["JOB_NAME"]

# Initialize Logger
logger = get_glue_logger()

def run_etl():
    try:
        print("Staring ETL Job " +args["JOB_NAME"])

        print("starting Puchase Behaviour    --->  ")
        print("S3 Target Path: " + s3_output_path)
        print("  starting transformation")


        customer_df = spark.read.table("bronze_db.customers_raw")
        order_df = spark.read.table("bronze_db.orders_raw")
        customer_df.createOrReplaceTempView("customers")
        order_df.createOrReplaceTempView("orders")

        customer_df.show()
        
        #common tranformation 
        top_customers=transform_top_customers_sql(spark)
        print("Running  SQL Query  for top customers    --->")


        top_customers.show()
        print(top_customers.count())
        #top_customers=transform_dataframe(order_df,customer_df)

        
        write_to_s3(top_customers,s3_output_path)

        print("ETL Job Completed Successfully")

    except Exception as e:
        print(f"ETL Job Failed: {str(e)}")
        raise e
    job.commit()

