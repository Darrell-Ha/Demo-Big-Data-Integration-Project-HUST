from pyspark.sql import SparkSession
from pyspark import SparkConf

def build_spark_session(app_name: str) -> SparkSession:
    """Build SparkSession with Iceberg config
    :param app_name: The name of the Spark application
    :type app_name: str
    :return: A SparkSession object
    :rtype: SparkSession
    """
    # conf = SparkConf.setAll(create_spark_iceberg_config().items())
    # spark = SparkSession.builder.appName(app_name).config(conf=conf).getOrCreate()
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    return spark
