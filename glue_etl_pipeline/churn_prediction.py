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
        order_df = spark.read.table("bronze_db.orders_raw")
        order_df.createOrReplaceTempView("orders")
        
        #common tranformation 
        churn_risk=transform_sql()
        
        #churn_risk=transform_dataframe(order_df)

        write_to_s3(churn_risk,s3_output_path)

        print("ETL Job Completed Successfully")
    except Exception as e:
        print(f"ETL Job Failed: {str(e)}")
        raise e
    job.commit()



def transform_sql():
        # Run Spark SQL Query
        churn_risk = spark.sql(""" 
                            WITH customer_activity AS (
                        SELECT
                            customer_id,
                            order_id,
                            order_date,
                            LAG(order_date) OVER (PARTITION BY customer_id ORDER BY order_date) AS prev_order_date
                        FROM orders
                    ),
                    churn_risk AS (
                        SELECT
                            customer_id,
                            COUNT(order_id) AS total_orders,
                            MAX(order_date) AS last_order_date,
                            DATEDIFF(current_date, MAX(order_date)) AS days_since_last_purchase,  
                            AVG(DATEDIFF(order_date, prev_order_date)) AS avg_order_gap  
                        FROM customer_activity
                        GROUP BY customer_id
                    )
                    SELECT *
                    FROM churn_risk
                    WHERE days_since_last_purchase > (avg_order_gap * 2)  -- Customers inactive for double their average gap
                    ORDER BY days_since_last_purchase DESC;

                                    """)
        return churn_risk


def transform_dataframe(order_df):
    
    # Define window specification for LAG function
    window_spec = Window.partitionBy("customer_id").orderBy("order_date")

    # Add previous order date using LAG function
    customer_activity = order_df.withColumn(
        "prev_order_date", F.lag("order_date").over(window_spec)
    )

    # Compute total orders, last order date, days since last purchase, and average order gap
    churn_risk = (
        customer_activity.groupBy("customer_id")
        .agg(
            F.count("order_id").alias("total_orders"),
            F.max("order_date").alias("last_order_date"),
            F.datediff(F.current_date(), F.max("order_date")).alias("days_since_last_purchase"),
            F.avg(F.datediff("order_date", "prev_order_date")).alias("avg_order_gap"),
        )
    )

    # Filter customers who are inactive for more than twice their average order gap
    churn_risk_filtered = churn_risk.filter(
        F.col("days_since_last_purchase") > (F.col("avg_order_gap") * 2)
    ).orderBy(F.desc("days_since_last_purchase"))

    # Show results
    churn_risk_filtered.show(10)

    return churn_risk_filtered