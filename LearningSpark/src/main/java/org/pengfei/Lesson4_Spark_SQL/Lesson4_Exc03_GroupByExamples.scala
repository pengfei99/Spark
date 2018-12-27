package org.pengfei.Lesson4_Spark_SQL

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.collect_list
import org.apache.spark.sql.functions.collect_set

object Lesson4_Exc03_GroupByExamples {
def main(args:Array[String]):Unit={
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  val spark=SparkSession.builder().appName("Test").master("local[2]").getOrCreate()
  import spark.implicits._

  val clusterLabels=spark.sparkContext.parallelize(Array((0,("normal",1)),(0,("normal",2)),(0,("back",1)),(1,("neptune",1)),(1,("satan",0)),(1,("nep",3)))).toDF("cluster","label").as[(Int,(String,Int))]


  /*First example, we groupBy on cluster, and concatane all elements of columne label in a new column "labels", here
  * we can use two pre-define functions*/

  // collect_list concatane all element without removing any
  val groupedClusterLabelsWithList=clusterLabels.groupBy("cluster").agg(collect_list("label").as("labels"))
  // collect_set concataine all but remove doubles, so only distinct value in the result array.
  val groupedClusterLabelsWithSet=clusterLabels.groupBy("cluster").agg(collect_set("label").as("labels"))

  groupedClusterLabelsWithList.show(false)
  groupedClusterLabelsWithSet.show(false)
}
}
