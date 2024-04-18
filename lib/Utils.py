from pyspark.sql import SparkSession
from lib.ConfigReader import get_spark_conf

def get_spark_session(job_run_env):
    
    if job_run_env == "LOCAL":
        return SparkSession.builder \
            .config(conf=get_spark_conf(job_run_env)) \
            .config("spark.driver.extraJavaOptions", "-Dlog4j.configuration=file:log4j.properties") \
            .master("local[2]") \
            .getOrCreate()
        
    else:
        return SparkSession.builder \
            .config(conf=get_spark_conf(job_run_env)) \
            .enableHiveSupport() \
            .getOrCreate()