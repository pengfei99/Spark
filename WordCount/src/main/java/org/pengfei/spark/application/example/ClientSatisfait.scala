package org.pengfei.spark.application.example


import breeze.numerics.round
//import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}

object ClientSatisfait {
def main(args:Array[String]): Unit ={
  /*val inputFile = "file:///tmp/satisfait.csv"
  val spark = SparkSession.builder().master("local").appName("ClientSatisfait").getOrCreate()
  val schema = dfSchema()
  val df = spark.read.format("com.databricks.spark.csv").option("header", "true").schema(schema).load(inputFile)*/
  //val managerList=df.select("manager_name").distinct().collectAsList()
  //println(managerList)

  //val Arjun=df.select("manager_name","satisfaction_level").groupBy("manager_name").count()
  //filter(df("manager_name")=== "Kabir Vish").
  //
  //df.show()
  //Arjun.show()


}

  def dfSchema():StructType={
    StructType(
      Seq(
        StructField(name = "manager_name", dataType = StringType, nullable = false),
        StructField(name = "client_name", dataType = StringType, nullable = false),
        StructField(name = "client_gender", dataType = StringType, nullable = false),
        StructField(name = "client_age", dataType = IntegerType, nullable = false),
        StructField(name = "response_time", dataType = DoubleType, nullable = false),
        StructField(name = "satisfaction_level", dataType = DoubleType, nullable = false)
      )
    )
  }

  // another way to write schema

 /* def simpleSchema():StructType={
    StructType(Array(
      StructField("manager_name",StringType,false),
      StructField("client_name",StringType,false),
      StructField("client_gender",StringType,false),
      StructField("client_age",IntegerType,false),
      StructField("response_time",DoubleType,false),
      StructField("satisfaction_level",DoubleType,false),
    ))
  }*/

}
