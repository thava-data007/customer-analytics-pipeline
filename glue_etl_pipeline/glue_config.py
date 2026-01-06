import os

#rds_endpoint="rds-mysql-instance.cjq8msca2hav.ap-south-1.rds.amazonaws.com"
rds_endpoint = "rds-mysql-instance.c1iogaq0g0r0.ap-south-1.rds.amazonaws.com"

# S3 Paths
S3_SOURCE_PATH = os.getenv("S3_SOURCE_PATH", "s3://may-mani-2025-training-bucket/raw-data/")
S3_TARGET_PATH = "s3://may-mani-2025-training-bucket/analytics"

# Glue Catalog Database & Table
GLUE_DATABASE = "customer_analytics"
GLUE_TABLE = "customer_transactions"


USER_MYSQL_URL = f"jdbc:mysql://{rds_endpoint}:3306/UserService"
ORDER_MYSQL_URL = f"jdbc:mysql://{rds_endpoint}:3306/OrderService"
PRODUCT_MYSQL_URL = f"jdbc:mysql://{rds_endpoint}:3306/ProductService"
MYSQL_PROPERTIES = {
    'user': 'root',
    'password': 'vmsKuOY~nk38|^l#~',
    'driver': 'com.mysql.cj.jdbc.Driver',
    'allowPublicKeyRetrieval':'true',
    'useSSL':'false'
}