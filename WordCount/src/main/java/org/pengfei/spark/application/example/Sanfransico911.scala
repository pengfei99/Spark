package org.pengfei.spark.application.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.types._
//dependencies for timestamp functions (e.g. year, totimestamp)
import org.apache.spark.sql.functions._

object Sanfransico911 {
def main (args:Array[String])={
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val spark = SparkSession.builder().
    master("local"). //spark://10.70.3.48:7077 remote
    appName("Sanfransico911").
    getOrCreate()
  import spark.implicits._
  /*
 * read data from csv file
 * */

  //val inputFile="hdfs://hadoop-nn.bioaster.org:9000/test_data/iot_devices.json"
 val inputFile="hdfs://localhost:9000/test_data/Fire_Department_Calls_for_Service.csv"

  val fireSchema = StructType(Array(
    StructField("CallNumber", IntegerType, true),
    StructField("UnitID", StringType, true),
    StructField("IncidentNumber", IntegerType, true),
    StructField("CallType", StringType, true),
    StructField("CallDate", StringType, true),
    StructField("WatchDate", StringType, true),
    StructField("ReceivedDtTm", StringType, true),
    StructField("EntryDtTm", StringType, true),
    StructField("DispatchDtTm", StringType, true),
    StructField("ResponseDtTm", StringType, true),
    StructField("OnSceneDtTm", StringType, true),
    StructField("TransportDtTm", StringType, true),
    StructField("HospitalDtTm", StringType, true),
    StructField("CallFinalDisposition", StringType, true),
    StructField("AvailableDtTm", StringType, true),
    StructField("Address", StringType, true),
    StructField("City", StringType, true),
    StructField("ZipcodeofIncident", IntegerType, true),
    StructField("Battalion", StringType, true),
    StructField("StationArea", StringType, true),
    StructField("Box", StringType, true),
    StructField("OriginalPriority", StringType, true),
    StructField("Priority", StringType, true),
    StructField("FinalPriority", IntegerType, true),
    StructField("ALSUnit", BooleanType, true),
    StructField("CallTypeGroup", StringType, true),
    StructField("NumberofAlarms", IntegerType, true),
    StructField("UnitType", StringType, true),
    StructField("Unitsequenceincalldispatch", IntegerType, true),
    StructField("FirePreventionDistrict", StringType, true),
    StructField("SupervisorDistrict", StringType, true),
    StructField("NeighborhoodDistrict", StringType, true),
    StructField("Location", StringType, true),
    StructField("RowID", StringType, true)))


  val df = spark.read.format("com.databricks.spark.csv").option("header", "true").schema(fireSchema).load(inputFile)
  //df.show(5)
  //df.printSchema()
  //println(df.getClass.getName)
 //Q1.How many different types of calls were made to the Fire Department
  //val calltype=df.select(df("CallType")).distinct()
  //println(calltype.count)
  //calltype.show(5)

  df.createOrReplaceTempView("911_call")
  //Q2. How many incidents of each call type were there?
  val callNumOfType=df.select(df("CallType"),df("RowID")).groupBy("CallType")

  //callNumOfType.sort($"count")
  val call=spark.sql("select CallType, count(distinct RowID) as call_num from 911_call group by CallType order by call_num desc limit 100")
  //call.show(5)
  //Q-3) How many years of Fire Service Calls is in the data file?
val call_year=df.select(df("CallDate"))
  //We can notice that the date is in string format, we need to convert them into date
  //call_year.show(5)
  val from_pattern1 = "MM/dd/yyyy"

  val to_pattern1 = "yyyy-MM-dd"
  val from_pattern2 = "MM/dd/yyyy hh:mm:ss aa"
  val to_pattern2 = "MM/dd/yyyy hh:mm:ss aa"
  val dateDf = df.withColumn("CallDateTS", to_timestamp($"CallDate", from_pattern1)).drop("CallDate")
    .withColumn("WatchDateTS", to_timestamp($"WatchDate", from_pattern1)).drop("WatchDate")
    .withColumn("ReceivedDtTmTS", to_timestamp($"ReceivedDtTm", from_pattern2)).drop("ReceivedDtTm")
    .withColumn("EntryDtTmTS", to_timestamp($"EntryDtTm", from_pattern2)).drop("EntryDtTm")
    .withColumn("DispatchDtTmTS", to_timestamp($"DispatchDtTm", from_pattern2)).drop("DispatchDtTm")
    .withColumn("ResponseDtTmTS", to_timestamp($"ResponseDtTm", from_pattern2)).drop("ResponseDtTm")
    .withColumn("OnSceneDtTmTS", to_timestamp($"OnSceneDtTm", from_pattern2)).drop("OnSceneDtTm")
    .withColumn("TransportDtTmTS", to_timestamp($"TransportDtTm", from_pattern2)).drop("TransportDtTm")
    .withColumn("HospitalDtTmTS", to_timestamp($"HospitalDtTm", from_pattern2)).drop("HospitalDtTm")
    .withColumn("AvailableDtTmTS", to_timestamp($"AvailableDtTm", from_pattern2)).drop("AvailableDtTm")

 // dateDf.select(year($"CallDateTS")).distinct().orderBy(year($"CallDateTS").desc).show()
  //dateDf.printSchema()

 // Q-4) How many service calls were logged in the past 7 days?
  // Suppose that today is July 6th, is the 187th day of the year. Filter the DF down to just 2016 and days of year greater than 180:

val last7day=dateDf.filter(year($"CallDateTS")===2016).filter(dayofyear($"CallDateTS")>=180).filter(dayofyear($"CallDateTS")<=187).select($"CallDateTS").distinct().orderBy($"CallDateTS".desc)
last7day.show(10)
}
}

/*
*|-- CallNumber: integer (nullable = true)
 |-- UnitID: string (nullable = true)
 |-- IncidentNumber: integer (nullable = true)
 |-- CallType: string (nullable = true)
 |-- CallDate: string (nullable = true)
 |-- WatchDate: string (nullable = true)
 |-- ReceivedDtTm: string (nullable = true)
 |-- EntryDtTm: string (nullable = true)
 |-- DispatchDtTm: string (nullable = true)
 |-- ResponseDtTm: string (nullable = true)
 |-- OnSceneDtTm: string (nullable = true)
 |-- TransportDtTm: string (nullable = true)
 |-- HospitalDtTm: string (nullable = true)
 |-- CallFinalDisposition: string (nullable = true)
 |-- AvailableDtTm: string (nullable = true)
 |-- Address: string (nullable = true)
 |-- City: string (nullable = true)
 |-- ZipcodeofIncident: integer (nullable = true)
 |-- Battalion: string (nullable = true)
 |-- StationArea: string (nullable = true)
 |-- Box: string (nullable = true)
 |-- OriginalPriority: string (nullable = true)
 |-- Priority: string (nullable = true)
 |-- FinalPriority: integer (nullable = true)
 |-- ALSUnit: boolean (nullable = true)
 |-- CallTypeGroup: string (nullable = true)
 |-- NumberofAlarms: integer (nullable = true)
 |-- UnitType: string (nullable = true)
 |-- Unitsequenceincalldispatch: integer (nullable = true)
 |-- FirePreventionDistrict: string (nullable = true)
 |-- SupervisorDistrict: string (nullable = true)
 |-- NeighborhoodDistrict: string (nullable = true)
 |-- Location: string (nullable = true)
 |-- RowID: string (nullable = true)
*
* */