package org.pengfei.Lesson04_Spark_SQL

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Lesson04_Exc02_UDAF {

  def main(args:Array[String]):Unit={
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val spark=SparkSession.builder().appName("Lesson04_Exc02_UDAF").master("local[2]").getOrCreate()
    import spark.implicits._
    val data=spark.sparkContext.parallelize(Array(
      ("B",List(3,4)),
      ("C",List(3,5)),
      ("A",List(2,6)),
      ("B",List(3,11,4,9)),
      ("C",List(5,6)),
      ("A",List(2,10,7,6))
    )).toDF("key","device")

    data.show()

    val mergeUDAF=new MergeListsUDAF()
    val dataAfterMerge=data.groupBy("key").agg(mergeUDAF($"device"))

    dataAfterMerge.show()
  }
}
