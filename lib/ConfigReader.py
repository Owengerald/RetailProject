import configparser
from pyspark import SparkConf

# loading the application configs in python dictionary
def get_app_conf(job_run_env):
    config_parser = configparser.ConfigParser()
    config_parser.read("configs/application.conf")
    app_conf = {}
    for (key, val) in config_parser.items(job_run_env):
        app_conf[key] = val
    return app_conf

# loading the pyspark configs and creating a spark conf object
def get_spark_conf(job_run_env):
    config_parser = configparser.ConfigParser()
    config_parser.read("configs/spark.conf")
    spark_conf = SparkConf()
    for (key, val) in config_parser.items(job_run_env):
        spark_conf.set(key, val)
    return spark_conf