package org.pengfei.spark.basics

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.split
import org.apache.spark.sql.types._
/*
* https://medium.com/@mrpowers/working-with-spark-arraytype-and-maptype-columns-4d85f3c8b2b3
* */
object DataFrameWithArrayTypeAndMapType {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val spark = SparkSession.builder().
    master("local[2]"). //spark://10.70.3.48:7077 remote
    appName("CalHousing").
    getOrCreate()
  // import sparkSession.implicits._ for all schema conversion magic.
  import spark.implicits._


  /*
  Exp1.

  val singersDF = Seq(("beatles", "help|hey jude"),
    ("romeo", "eres mia")).toDF("name", "hit_songs")

  singersDF.show()
  print(singersDF.getClass.getName)
  val actualDF = singersDF.withColumn("hit_songs", split(singersDF("hit_songs"),"\\|"))

  actualDF.show()*/

  /*val data = Seq(("bieber", Array("baby", "sorry")),
    ("ozuna", Array("criminal")))

  val schema = List(
    StructField("name", StringType, true),
    StructField("hit_songs", ArrayType(StringType, true), true)
  )

  val singersDF = spark.createDataFrame(
    spark.sparkContext.parallelize(data),StructType(schema))*/

  val someData = Seq(
    Row(8, "bat"),
    Row(64, "mouse"),
    Row(-27, "horse")
  )

  val someSchema = List(
    StructField("number", IntegerType, true),
    StructField("word", StringType, true)
  )

  val someDF = spark.createDataFrame(
    spark.sparkContext.parallelize(someData),
    StructType(someSchema)
  )



}
