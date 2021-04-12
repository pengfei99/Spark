import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkFiles


def get_shape(df, fmt):
    row_num = df.count()
    col_num = len(df.columns)
    print("The data frame has {} rows and {} columns".format(row_num, col_num))
    return row_num, col_num


def mute_spark_logs(sc):
    """Mute Spark info logging"""
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)


if __name__ == "__main__":
    data_url = (
        "https://minio.lab.sspcloud.fr/pengfei/sspcloud-demo/pokemon-cleaned.csv"
    )
    spark = SparkSession.builder.master("local[*]").getOrCreate()
    mute_spark_logs(spark.sparkContext)