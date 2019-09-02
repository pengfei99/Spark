package org.pengfei.Lesson06_Spark_Streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/** In this exercise, we use a flume data source which output data to local socket 6666.
  * The objective of this exercise is to use scala streaming to read and process data sent
  * by flume.
  *
  * We suppose you have a flume agent which can output data, if not, see
  * https://35.243.150.190/doku.php?id=employes:pengfei.liu:data_science:flume:start#flume_working_examples
  * to build a flume agent
  *
  * In lesson6_1, we already used spark flume streaming (data is stored as RDD). Here, we will use the spark structure
  * streaming (data is stored as DataSet/DataFrame),
  * */
object Lesson06_4_Exo2 {
  def main(args:Array[String])={
    /* data source config*/
    val host="127.0.0.1"
    val port=6666

    /* build spark session*/
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val spark=SparkSession.builder.appName("Lesson6_5_Spark_Structure_Streaming").master("local[2]").getOrCreate()
    import spark.implicits._

    /* Read data from flume */
    val lines=spark.readStream.format("socket").option("host",host).option("port",port).load()

    //spark.readStream returns a untyped dataframe.
    println(s"${lines.getClass.getName}")
    lines.isStreaming    // Returns True for DataFrames that have streaming sources
    lines.printSchema
    // As the dataframe is untyped, meaning that the schema of the Dataframe is not checked at compile time.
    // only checked at runtime when the query is submitted. Some operations like map, flatMap, etc. need the type
    // to be known at compile time. The below example, to use flatmap, we have converted the DataFrame to a
    // Dataset of String using .as[String]
    val words=lines.as[String].flatMap(_.split(" "))

    val wordCounts = words.groupBy("value").count()
    val query=wordCounts.writeStream.outputMode("complete").format("console").start()

    query.awaitTermination()
  }

}
