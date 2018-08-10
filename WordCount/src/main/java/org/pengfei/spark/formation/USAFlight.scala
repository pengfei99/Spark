package org.pengfei.spark.formation



import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object USAFlight {
  def main(args:Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = SparkSession.builder().
      master("local[2]"). //spark://10.70.3.48:7077 remote
      appName("USAFlight").
      getOrCreate()
    //spark.conf.set("")
    import spark.implicits._

    val inputFile = "file:////home/pliu/Downloads/flight.csv"
/*
* DEST_COUNTRY_NAME,ORIGIN_COUNTRY_NAME,count

* */
    val schema = StructType(Array(
      StructField("DEST_COUNTRY_NAME", StringType, true),
      StructField("ORIGIN_COUNTRY_NAME", StringType, true),
      StructField("COUNT", IntegerType, true)))

    val flightDF=spark.read.format("csv").option("delimiter", ",").schema(schema).load(inputFile)

    //flightDF.show(10)

/* sort the data set by flight number*/

    val origin_usa=flightDF.filter($"ORIGIN_COUNTRY_NAME"==="United States").orderBy(flightDF("COUNT").desc)
    //origin_usa.show(5)

    /* count the total number of flight arrive or level*/
   val count_leave_usa=origin_usa.agg(sum("COUNT").cast("long")).first.getLong(0)
    print("Leave usa:"+count_leave_usa)
    val dest_usa=flightDF.filter($"DEST_COUNTRY_NAME"==="United States").orderBy(flightDF("COUNT").desc)
    val count_dest_usa=dest_usa.agg(sum("COUNT").cast("long")).first().getLong(0)
    print("Come in usa"+count_dest_usa)
    val total=count_leave_usa+count_dest_usa
    print("Total flights of usa"+total)
  }

  def lineWordCount(text: String): Long={

    val word=text.split(" ").map(_.toLowerCase).groupBy(identity).mapValues(_.size)
    val counts=word.foldLeft(0){case (a,(k,v))=>a+v}
    /* print(word)
     print(counts)*/
    return counts

  }
}

