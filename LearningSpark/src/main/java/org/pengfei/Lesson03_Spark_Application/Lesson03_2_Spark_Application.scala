package org.pengfei.Lesson03_Spark_Application

import org.apache.spark.sql.SparkSession

object Lesson03_2_Spark_Application {

  def main(args: Array[String]) = {



  /** *****************************Spark API Entry Point: SparkSession ****************************/
  /* Since spark 2.0+, SparkSession is the entry point into all spark functionality. It replace all the other
  * entry point for spark core (sparkContext), spark-sql(sqlContext/hiveContext). To create a basic sparkSession,
  * just use SparkSession.builder()*/

  val spark=SparkSession
    .builder()
    .master("local[2]")
    .appName("Lesson3_2_Spark_Application")
    .getOrCreate()

    //for implicit conversions like converting RDDs to dataframes
    import spark.implicits._
}
}
