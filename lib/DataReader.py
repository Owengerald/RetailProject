from lib import ConfigReader
from lib import Utils

#defining customers schema
def get_customers_schema():
    schema = """customer_id int, customer_fname string, customer_lname string, 
                username string, password string, address string, 
                city string, state string,pincode string"""
    return schema

# creating customers dataframe
def read_customers(spark, job_run_env):
    app_conf = ConfigReader.get_app_conf(job_run_env)
    customers_file_path = app_conf["customers.file.path"]
    return spark.read \
        .format("csv") \
        .option("header", "true") \
        .schema(get_customers_schema()) \
        .load(customers_file_path)

#defining orders schema
def get_orders_schema():
    schema = "order_id int, order_date string, customer_id int, order_status string"
    return schema

#creating orders dataframe
def read_orders(spark, job_run_env):
    app_conf = ConfigReader.get_app_conf(job_run_env)
    orders_file_path = app_conf["orders.file.path"]
    return spark.read \
        .format("csv") \
        .option("header", "true") \
        .schema(get_orders_schema()) \
        .load(orders_file_path)