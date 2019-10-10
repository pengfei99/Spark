package org.pengfei.Lesson04_Spark_SQL

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Lesson04_Exc04_Parse_Apache_Access_Log {

  def main(args:Array[String]):Unit={
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val spark = SparkSession.builder().master("local[2]").appName("Lesson4_Exec04_Parse_Apache_Access_Log").getOrCreate()

    import spark.implicits._
    val sparkConfig = ConfigFactory.load("application.conf").getConfig("spark")
    val  path= sparkConfig.getString("sourceDataPath")
    val filePath=s"${path}/spark_lessons/Lesson04_Spark_SQL/access.log.2"
    // Read raw data
    val rawDf=spark.read.text(filePath)

    rawDf.cache()
    rawDf.show(5,false)
    rawDf.count()

    // split raw data into token list
    val splitDf=rawDf.select(split(col("value")," ").as("tokenList"))
    splitDf.show(5,false)

    // transform token list into dataframe
    val tokenizedDf=splitDf.withColumn("host",$"tokenList".getItem(0))
      .withColumn("rfc931",$"tokenList".getItem(1))
      .withColumn("authuser",$"tokenList".getItem(2))
      .withColumn("date",concat($"tokenList".getItem(3),$"tokenList".getItem(4)))
      .withColumn("request",$"tokenList".getItem(6))
      .withColumn("status",$"tokenList".getItem(8))
      .withColumn("bytes",$"tokenList".getItem(9))
      .drop("tokenList")

    tokenizedDf.show(5,false)

    tokenizedDf.select("status").distinct()

    /* The following request give us the top ten most visited page. We could noticed that the second most viewed item is not in the top 10 sell list */
    val mostViewedPage=tokenizedDf.filter($"request".contains("product")).groupBy($"request").count().orderBy($"count".desc)

    mostViewedPage.show(10,false)

    /* If we want to replace the 20% by space in the request, we can use the regexp_replace*/
    val betterView=mostViewedPage.select(regexp_replace($"request","%20"," ").alias("request"),$"count")
    betterView.show(10,false)


  }
}
