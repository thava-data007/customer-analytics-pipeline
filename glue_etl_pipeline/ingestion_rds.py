import sys
import boto3
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql import functions as F
from pyspark.sql.window import Window
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
        customer_df =read_from_rds(spark,USER_MYSQL_URL,"Customers")
        products_df =read_from_rds(spark,PRODUCT_MYSQL_URL,"Products")
        order_df =read_from_rds(spark,ORDER_MYSQL_URL,"Orders")
        order_items_df =read_from_rds(spark,ORDER_MYSQL_URL,"Order_Items")
        user_logins_df =read_from_rds(spark,USER_MYSQL_URL,"LoginHistory")
   

        write_to_s3(customer_df,s3_output_path+"/customers_raw")
        write_to_s3(products_df,s3_output_path+"/products_raw")
        write_to_s3(order_df,s3_output_path+"/orders_raw")
        write_to_s3(order_items_df,s3_output_path+"/order_items_raw")
        write_to_s3(user_logins_df,s3_output_path+"/login_history_raw")

        print("ETL Job Completed Successfully")
    except Exception as e:
        print(f"ETL Job Failed: {str(e)}")
        raise e
    job.commit()
