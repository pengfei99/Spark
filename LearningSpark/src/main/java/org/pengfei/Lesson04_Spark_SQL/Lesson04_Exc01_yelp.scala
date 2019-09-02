package org.pengfei.Lesson04_Spark_SQL

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Lesson04_Exc01_yelp {
def main(args:Array[String])={
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  /*In this exc01, I will use a yelp data set to illustrate how to do data analytics with spark*/
  val filePath="/home/pliu/data_set/spark_data_set/spark_lessons/Lesson04_Spark_SQL/yelp_academic_dataset_business.json"
  val spark=SparkSession.builder().master("local[2]").appName("Lesson4_Exc01_yelp").getOrCreate()

  val df=spark.read.option("inferSchema","true").json(filePath)
  df.show(1,false)

  //val df1=spark.read.format("json").option("inferSchema","true").load(filePath)
  //df1.show(1,false)

  /*************************************************************************************************
    * ***************************Step 1. Understand your dataset************************************
    *************************************************************************************************/

  /********************************1.1 Check dataset schema***************************************/
/* Here I ask spark to infer the schema from json file, so we need to check the schema, if you define the
* schema yourself, you don't need to do this*/

  df.printSchema()

  /*******************************1.2 get dataset size******************************************/
  // get dataset row numbers
  val rowNum=df.count()
  // get column numbers
  val colNum=df.columns.length

  println(s"data set has ${rowNum} rows and ${colNum} columns")


}
}
