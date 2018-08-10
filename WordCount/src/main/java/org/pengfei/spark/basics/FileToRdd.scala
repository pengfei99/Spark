package org.pengfei.spark.basics

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.typedLit


object FileToRdd {
  def main(args:Array[String])={
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = SparkSession.builder().
      master("local[2]"). //spark://10.70.3.48:7077 remote
      appName("CalHousing").
      getOrCreate()
    // import sparkSession.implicits._ for all schema conversion magic.
    import spark.implicits._

    val file1="/DATA/data_set/spark/basics/Lesson1_RDD/file1.txt"
    val file2="/DATA/data_set/spark/basics/Lesson1_RDD/file2.txt"

    // in spark 2.2. spark session read will return dataset. here we don't provide schema,
    // so it returns a dataset with one column (value) as type String
    val data1=spark.read.text(file1).as[String]
    val data2=spark.read.text(file2).as[String]
    data1.show()

    //This step transform string "1,9,5" to a string list ["1","9","5"]
    val values1=data1.map(value => value.split(","))

    //This step transform column value ["1","9","5"] to three column
    val df1=values1.select($"value".getItem(0).as("col1"),$"value".getItem(1).as("col1"),$"value".getItem(2).as("col2"))
    // val df1=values1.select($"value".getItem(0).as("col1"),typedLit(Seq($"value".getItem(1).as[String],$"value".getItem(2).as[String])).as("col2"))
    df1.show()

    val rdd1= df1.rdd
    print(rdd1)



  }

}
