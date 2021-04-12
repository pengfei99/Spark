import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkFiles


def saveCSV(df, outputPath, fileName): Unit = {
    df.coalesce(1).write.mode("overwrite")
        .option("header", "true")
        .option("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
        .option("encoding", "UTF-8")
        .option("delimiter", ",")
        .csv(outputPath + "/" + fileName)
}


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
