package org.pengfei.spark.basics

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object MultiDimentionalAgg {

  def main(args: Array[String])={
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = SparkSession.builder().
      master("local[2]"). //spark://10.70.3.48:7077 remote
      appName("USAFlight").
      getOrCreate()
    //spark.conf.set("")
    import spark.implicits._
    val sales = Seq(
      ("Warsaw", 2016, 100),
      ("Warsaw", 2017, 200),
      ("Boston", 2015, 50),
      ("Boston", 2016, 150),
      ("Toronto", 2017, 50)
    ).toDF("city", "year", "amount")

    sales.show()

    val groupByCityAndYear = sales.groupBy("city","year").agg(sum("amount") as "amount")
    groupByCityAndYear.show()
    val total_amount=sales.agg(sum("amount").cast("long")).first.getLong(0)
    print(total_amount)
  }
}
