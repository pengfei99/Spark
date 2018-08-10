package org.pengfei.spark.application.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object TaobaoSales {

def main(args:Array[String]): Unit ={
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val spark = SparkSession.builder().
    master("local").
    appName("TaobaoSales").
    getOrCreate()

  val userLogDF=getDFFromDB(spark)
  /*val filePath="file:///DATA/data_set/spark/taobao_data_set/small_user_log.csv"
  val userLogDF=getDFFromCSV(spark,filePath)*/
  //userLogDF.show(5)
  userLogDF.write.format("parquet").save("file:///tmp/taobao.parquet")

}

  def getDFFromDB(spark : SparkSession): DataFrame ={
    val userLogDF=spark.read.format("jdbc").option("url", "jdbc:postgresql://127.0.0.1:5432/dbtaobao").option("driver","org.postgresql.Driver").option("dbtable", "user_log").option("user", "pliu").option("password", "Liua1983").load()
    return userLogDF
  }

  def getDFFromCSV(spark:SparkSession,filePath:String):DataFrame ={
    val userLogSchema = StructType(Array(
      StructField("user_id",IntegerType,true),
      StructField("item_id",IntegerType,true),
      StructField("cat_id",IntegerType,true),
      StructField("merchant_id",IntegerType,true),
      StructField("brand_id",IntegerType,true),
      StructField("month",StringType,true),
      StructField("day",StringType,true),
      StructField("action",IntegerType,true),
      StructField("age_range",IntegerType,true),
      StructField("gender",IntegerType,true),
      StructField("province",StringType,true)
    ))
    val userLogDF= spark.read.format("csv").option("header","false").schema(userLogSchema).load(filePath)
    return userLogDF
  }

}
