import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkFiles
from pyspark.sql.functions import ltrim, rtrim, trim
import pyspark.sql.functions as f

# This function removes space in a cell of string type.
# df is the data frame, col_name is the column name
# position defines where you want to remove the space.
# if "l", remove left side space
# if "r", remove right side space
# if "a", remove both sides space
def remove_space(df, col_name, position):
    # remove left side space
    if position == "l":
        return df.withColumn("tmp", ltrim(f.col(col_name))).drop(col_name).withColumnRenamed("tmp", col_name)
    # remove right side space
    elif position == "r":
        return df.withColumn("tmp", rtrim(f.col(col_name))).drop(col_name).withColumnRenamed("tmp", col_name)
    # remove all side space
    elif position == "a":
        return df.withColumn("tmp", trim(f.col(col_name))).drop(col_name).withColumnRenamed("tmp", col_name)

# This function filter a column of a data frame which value is in the value list
# df is a data frame, col_name is the name of the column, value_list is a list of int
def filter_pokemon_by_generation(df, col_name, value_list):
    return df.filter(f.col(col_name).isin(value_list))

# This function can extract a file name from a full file path
# path is the column name where stores the full file path
def extract_file_name(path):
    return sql_fun.substring_index(path, "/", -1)

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
