import sys
import boto3
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql import functions as F
from pyspark.sql.functions import col, count, sum, when, coalesce, desc, date_sub, current_date
from pyspark.sql.window import Window
from glue_etl_pipeline.utils import get_glue_logger,read_from_rds,write_to_s3,read_from_dynamodb
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
        
        customersupport_df=read_from_dynamodb(glueContext,"CustomerSupport","ap-south-1")
        enterprisecampaigns_df=read_from_dynamodb(glueContext,"EnterpriseCampaigns","ap-south-1")
       
        write_to_s3(customersupport_df,s3_output_path+"/customer_support_raw")
        write_to_s3(enterprisecampaigns_df,s3_output_path+"/enterprise_campaign_raw")

        print("ETL Job Completed Successfully")
    except Exception as e:
        print(f"ETL Job Failed: {str(e)}")
        raise e
    job.commit()






