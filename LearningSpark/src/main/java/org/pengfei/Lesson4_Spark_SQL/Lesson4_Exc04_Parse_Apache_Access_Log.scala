package org.pengfei.Lesson4_Spark_SQL

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Lesson4_Exc04_Parse_Apache_Access_Log {

  def main(args:Array[String]):Unit={
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val spark = SparkSession.builder().master("local[2]").appName("Lesson4_Exec04_Parse_Apache_Access_Log").getOrCreate()

    import spark.implicits._
    val filePath="/DATA/data_set/spark/basics/Lesson4_Spark_SQL/access.log.2"

    val rawDf=spark.read.text(filePath)

    rawDf.show(5,false)

    val splitDf=rawDf.select(split(col("value")," ").as("tokenList"))

    splitDf.show(5,false)

   val tokenizedDf=splitDf.withColumn("host",$"tokenList".getItem(0))
     .withColumn("rfc931",$"tokenList".getItem(1))
     .withColumn("authuser",$"tokenList".getItem(2))
     .withColumn("date",concat($"tokenList".getItem(3),$"tokenList".getItem(4)))
     .withColumn("request",$"tokenList".getItem(6))
     .withColumn("status",$"tokenList".getItem(8))
     .withColumn("bytes",$"tokenList".getItem(9))
       .drop("tokenList")


    tokenizedDf.show(5,false)

    val mostViewedPage=tokenizedDf.filter($"request".contains("product")).groupBy($"request").count().orderBy($"count".desc)

    mostViewedPage.show(10,false)

    /* If we want to replace the 20% by space in the request, we can use the regexp_replace*/
    val betterView=mostViewedPage.select(regexp_replace($"request","%20"," ").alias("request"),$"count").show(10,false)

  }
}
