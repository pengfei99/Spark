package org.pengfei.Lesson06_Spark_Streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.scalalang.typed
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object Lesson06_5_5_Operations_On_Streaming_DF {

val host="localhost"
  val port=9999

  def main(args:Array[String])={
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val spark=SparkSession.builder.appName("Lesson6_5_5_Operations_On_Streaming_DF").master("local[2]").getOrCreate()

    /*****************************************************************************************************************
      * ***************************** 6.5.5 Operations on streaming Dataframe/DataSets *******************************
      * ***************************************************************************************************************/

    /***********************************6.5.5.1 Basic Operations******************************************************/

    /* Most of the common dataframe/set operations are supported for streaming. The few operations that are not
    * supported :
    * 1. Multiple streaming aggregations (i.e. a chain of aggregations on a streaming DF) are not yet supported
    *    on streaming datasets.
    * 2. Limit and first N rows are not supported on streaming datasets
    * 3. Distinct operations on streaming are not supported
    * 4. Sorting operations are supported on streaming datasets only after an aggregation and in Complete output mode
    * 5. Few types of outer joins on streaming dataset are not supported. See section 6.5.5.1.1
    *
    * In addition, there are some Dataset methods that will not work on streaming Datasets. They are actions that
    * will immediately run queries and return results, which does not make sense on a streaming Dataset. Rather, those
    * functionalities can be done by explicitly starting a streaming query (see the next section regarding that).
    *
    * count() - Cannot return a single count from a streaming Dataset. Instead, use ds.groupBy().count() which returns
    *           a streaming Dataset containing a running count.
    *
    * foreach() - Instead use ds.writeStream.foreach(...) (see next section).
    *
    * show() - Instead use the console sink (see next section).
    *
    * If you try any of these operations, you will see an AnalysisException like “operation XYZ is not supported with
    * streaming DataFrames/Datasets”. While some of them may be supported in future releases of Spark, there are others
    * which are fundamentally hard to implement on streaming data efficiently. For example, sorting on the input stream
    * is not supported, as it requires keeping track of all the data received in the stream. This is therefore
    * fundamentally hard to execute efficiently.*/


    /*********************************6.5.5.1.1 Support matrix for joins in streaming queries*************************/
    /* We can do joins between static and stream dataframe, below is the matrix which describe which join is supported
    *
    * Left Input 	    |    Right Input  	|   Join Type    |     Support or not
    *   Static	      |      Static	      |   All types	   | Supported, since its not on streaming data even though it can be present in a streaming query
    *   Stream	      |      Static	      |    Inner	     | Supported, not stateful
    *                 |                   |   Left Outer	 | Supported, not stateful
    *                 |                   |  Right Outer	 | Not supported
    *                 |                   |  Full Outer	   | Not supported
    *   Static	      |      Stream	      |    Inner	     | Supported, not stateful
    *                 |                   |   Left Outer	 | Not supported
    *                 |                   |   Right Outer	 | Supported, not stateful
    *                 |                   |   Full Outer	 | Not supported
    *   Stream	      |      Stream	      |     Inner	     | Supported, optionally specify watermark on both sides + time constraints for state cleanup
    *                 |                   |   Left Outer	 | Conditionally supported, must specify watermark on right + time constraints for correct results, optionally specify watermark on left for all state cleanup
    *                 |                   |   Right Outer	 | Conditionally supported, must specify watermark on left + time constraints for correct results, optionally specify watermark on right for all state cleanup
    *                 |                   |    Full Outer	   | Not supported
    *
    *  Additional details on supported joins:
    *
    *  - Joins can be cascaded, that is, you can do df1.join(df2, ...).join(df3, ...).join(df4, ....).
    *  - As of Spark 2.3, you can use joins only when the query is in Append output mode. Other output modes are
    *    not yet supported.
    *  - As of Spark 2.3, you cannot use other non-map-like operations before joins. Here are a few examples of
    *    what cannot be used.
    *       - Cannot use streaming aggregations before joins.
    *       - Cannot use mapGroupsWithState and flatMapGroupsWithState in Update mode before joins               */

    /******************************6.5.5.1.2 Selection/Projection/aggregation *****************************************/

   // BasicOperationExample(spark)

    /********************************6.5.5.1.3 Stateless Aggregations **********************************************/
    /* In structured streaming, all aggregations are stateful by default. As we saw in BasicOperationExample when we do
     * groupBy and count on dataframe, spark remembers the state from the beginning. Also we write the complete
    * output every time when we receive the data as state keeps on changing.
    *
    *  Most of the time, the scenarios of stream processing need to be stateful, it comes with the cost of state
    *  management and state recovery in the case of failures. So if we are doing simple ETL processing on stream,
    *  we may not need state to be kept across the stream. Sometime we want to keep the state just for small batch
    *  and then reset.
    *
    *  For example, let’s say we want to count the words for every 5 seconds. Here the aggregation is done on
    *  the data stream which is collected for last 5 seconds. The state is only kept for those 5 seconds and then
    *  forgotten. So in case of failure, we need to recover data only for last 5 seconds.
    *  */

    // StatelessAggExample(spark)

    /******************************** 6.5.5.2 Window operations on Event Time ****************************************/

    /* Aggregations over a sliding event-time window are straightforward with Structured Streaming and are very
    * similar to grouped aggregations. In a grouped aggregation, aggregate values (e.g. counts) are maintained
    * for each unique value in the user-specified grouping column. In case of window-based aggregations,
    * aggregate values are maintained for each window the event-time of a row falls into. Let’s understand
    * this with an illustration.
    *
    * Imagine data stream now contains lines along with the time when the line was generated. Instead of running
    * word counts, we want to count words within 10 minute windows, updating every 5 minutes. That is, word counts
    * in words received between 10 minute windows 12:00 - 12:10, 12:05 - 12:15, 12:10 - 12:20, etc. Note that
    * 12:00 - 12:10 means data that arrived after 12:00 but before 12:10. Now, consider a word that was received
    * at 12:07. This word should increment the counts corresponding to two windows 12:00 - 12:10 and 12:05 - 12:15.
    * So the counts will be indexed by both, the grouping key (i.e. the word) and the window (can be calculated from
    * the event-time).*/

    // WindowOperationExample(spark)

    /***************************** 6.5.5.3 Handling late data and watermarking ********************************/

/* Now consider what happens if one of the events arrives late to the application. For example, say, a word generated
* at 12:04 (i.e. event time) could be received by the application at 12:11. The application should use the time 12:04
* instead of 12:11 to update the older counts for the window 12:00 - 12:10. This occurs naturally in our window-based
* grouping – Structured Streaming can maintain the intermediate state for partial aggregates for a long period of
* time such that late data can update aggregates of old windows correctly, as illustrated below.
*
* However, to run this query for days, it’s necessary for the system to bound the amount of intermediate in-memory
* state it accumulates. This means the system needs to know when an old aggregate can be dropped from the in-memory
* state because the application is not going to receive late data for that aggregate any more. To enable this,
* in Spark 2.1, we have introduced watermarking, which lets the engine automatically track the current event
* time in the data and attempt to clean up old state accordingly. You can define the watermark of a query by
* specifying the event time column and the threshold on how late the data is expected to be in terms of event time.
* For a specific window starting at time T, the engine will maintain state and allow late data to update the state
* until (max event time seen by the engine - late threshold > T). In other words, late data within the threshold
* will be aggregated, but data later than the threshold will start getting dropped (see later in the section for
* the exact guarantees). Let’s understand this with an example. We can easily define watermarking on the previous
* example using withWatermark() as shown below.
* */
   // WaterMarkOperationExample(spark)

    /* Some sinks (e.g. files) may not supported fine-grained updates that Update Mode requires. To work with them,
    * we have also support Append Mode, where only the final counts are written to sink.
    *
    * Similar to the Update Mode earlier, the engine maintains intermediate counts for each window. However,
    * the partial counts are not updated to the Result Table and not written to sink. The engine waits for
    * “10 mins” for late date to be counted, then drops intermediate state of a window < watermark,
    * and appends the final counts to the Result Table/sink. For example, the final counts of window 12:00 - 12:10 is
    * appended to the Result Table only after the watermark is updated to 12:11.
    *
    * Note that using withWatermark on a non-streaming Dataset is no-op. As the watermark should not affect any batch
    * query in any way, we will ignore it directly.*/

    /* Conditions for watermarking to clean aggregation state
    *
    * It is important to note that the following conditions must be satisfied for the watermarking to clean the
    * state in aggregation queries (as of Spark 2.1.1, subject to change in the future)
    * - output mode must be Append or Update. Complete mode requires all aggregate data to be preserved,
    *   and hence cannot use watermarking to drop intermediate state. See the Output Modes section for
    *   detailed explanation of the semantics of each output mode.
    *
    * - The aggregation must have either the event-time column, or a window on the event-time column.
    *
    * - withWatermark must be called on the same column as the timestamp column used in the aggregate.
    *   For example, df.withWatermark("time", "1 min").groupBy("time2").count() is invalid in Append output mode,
    *   as watermark is defined on a different column from the aggregation column.
    *
    * - withWatermark must be called before the aggregation for the watermark details to be used. For example,
    *   df.groupBy("time").count().withWatermark("time", "1 min") is invalid in Append output mode.
    *   */

    /* Semantic Guarantees of Aggregation with Watermarking
    *
    * - A watermark delay (set with withWatermark) of “2 hours” guarantees that the engine will never drop any data
    *   that is less than 2 hours delayed. In other words, any data less than 2 hours behind (in terms of event-time)
    *   the latest data processed till then is guaranteed to be aggregated.
    *
    * - However, the guarantee is strict only in one direction. Data delayed by more than 2 hours is not guaranteed
    *   to be dropped; it may or may not get aggregated. More delayed is the data, less likely is the engine going
    *   to process it.*/

    /***********************************6.5.5.4 Join Operations******************************************************/

/* Structured Streaming supports joining a streaming Dataset/DataFrame with a static Dataset/DataFrame as well as
 * another streaming Dataset/DataFrame. The result of the streaming join is generated incrementally, similar to the
 * results of streaming aggregations in the previous section. In this section we will explore what type of joins
 * (i.e. inner, outer, etc.) are supported in the above cases. Note that in all the supported join types, the result
 * of the join with a streaming Dataset/DataFrame will be the exactly the same as if it was with a static
 * Dataset/DataFrame containing the same data in the stream.
 *
 * But there are limits on join operations, see section "6.5.5.1.1 Support matrix for joins" for join support.
 *
 * In spark2.3, spark added support for stream-stream joins, the challenge of generating join results between two
 * data stream is that, at any point of time, the view of the dataset is incomplete for both sides of the join. As
 * a result, it's hard to find matches between inputs. */
  }

  def BasicOperationExample(spark:SparkSession):Unit={
    //process data from socket source
   // BasicOperationOnSocket(spark)
    //process data from file source
  // BasicOperationOnFile(spark)
  }

  def BasicOperationOnSocket(spark:SparkSession):Unit={
    import spark.implicits._
    val lines= spark.readStream.format("socket").option("host",host).option("port",port).load()
    lines.isStreaming    // Returns True for DataFrames that have streaming sources
    //lines.printSchema
    val devices:Dataset[DeviceData]=lines.as[String].map{_.split(";") match {
      case Array(device,deviceType,signal,time)=>DeviceData(device,deviceType,signal.toDouble,time)
    }}

    //  devices.printSchema()
    val meanTemp=devices.groupBy($"device").mean("signal")
    val query=devices.writeStream.outputMode("update").format("console").start()
    val query1=meanTemp.writeStream.outputMode("complete").format("console").start()

    query.awaitTermination()
    query1.awaitTermination()
  }

  def BasicOperationOnFile(spark:SparkSession):Unit={
    import spark.implicits._
        val deviceSchema=new StructType().add("device","string").add("deviceType","string").add("signal","double").add("time","string")

        val schema=StructType(Array(
           StructField("device",StringType,false),
          StructField("deviceType",StringType,false),
          StructField("signal",DoubleType,false),
          StructField("time",StringType,false)
        ))
    val path="/DATA/data_set/spark/basics/Lesson6_Spark_Streaming/structrued_streaming/device"
    //df untyped
    val df:DataFrame=spark.readStream.option("sep",";").schema(deviceSchema).csv(path)
    df.printSchema()

    //We can also create temp or global view for sql query
    df.createOrReplaceTempView("device")
    // all query on streaming data frame view also return another streaming df
    val countDevice=spark.sql("select count(*) from device")

    val ds:Dataset[DeviceData]=df.as[DeviceData]
    // select devices which have signal more than 27

    val d1=df.select("device").where("signal>27")
    val d2=ds.filter(_.signal>27).map(_.device)
    // average signal for each device
    val d3=ds.groupByKey(_.device).agg(typed.avg(_.signal))
    val query=d3.writeStream.outputMode("update").format("console").start()
    query.awaitTermination()
  }

  case class DeviceData(device:String, deviceType:String,signal:Double,time:String)

  def StatelessAggExample(spark: SparkSession):Unit ={
    import spark.implicits._
    val lines=spark.readStream.format("socket").option("host",host).option("port",port).load()

    val words=lines.as[String].flatMap(_.split(";"))

    /* Rather than using groupBy API of dataframe, we use groupByKey from the dataset. As we need to group on words,
    * we just pass the same value to grouping function. If you have complex object, then you can choose which
    * column you want to treat as the key.
    *
    * flatMapGroups is an aggregation API which applies a function to each group in the dataset. It’s
    * only available on grouped dataset. This function is very similar to reduceByKey of RDD world which allows
    * us to do arbitrary aggregation on groups.
    *
    * In our example, we apply a function for every group of words, we do the count for that group.
    *
    * One thing to remember is flatMapGroups is slower than count API. The reason being flatMapGroups doesn’t
    * support the partial aggregations which increase shuffle overhead. So use this API only to do small batch
    * aggregations. If you are doing aggregation across the stream, use the stateful operations.
    * */
    val countDs = words.groupByKey(value=>value).flatMapGroups{
      case (value, iter)=> Iterator((value, iter.length))
    }.toDF("value", "count")

    val query = countDs.writeStream.format("console").outputMode("append").trigger(Trigger.ProcessingTime("5 seconds")).start()

    query.awaitTermination()
  }

  def WindowOperationExample(spark:SparkSession):Unit={
    import spark.implicits._
    //streaming DataFrame of schema { word: String, timestamp: String }
    val lines=spark.readStream.format("socket").option("host",host).option("port",port).load()
    /* Since this windowing is similar to grouping, in code, you can use groupBy() and window() operations to
    * express windowed aggregations. */
    val words=lines.as[String].map{_.split(";") match {
      case Array(word,timestamp)=>WordWithTime(word,timestamp)
    }}
    //convert string time to timestamp, now streaming DataFrame of schema { word: String, timestamp: Timestamp }
    val wordsTime=words.select($"word",unix_timestamp($"time","yyyy/MM/dd HH:mm:ss").cast(TimestampType).as("timestamp"))

    val windowedCounts = wordsTime.groupBy(window($"timestamp","10 minutes","5 minutes"),$"word").count()

    val query= windowedCounts.writeStream.format("console").outputMode("append").start()
    query.awaitTermination()
  }


  def WaterMarkOperationExample(spark:SparkSession):Unit={
    import spark.implicits._
    val lines:DataFrame = spark.readStream.format("socket").option("host",host).option("port",port).load()
    val words:Dataset[WordWithTime] = lines.as[String].map{_.split(";") match {
      case Array(word,timestamp)=>WordWithTime(word,timestamp)
    }}
    //convert string time to timestamp, now streaming DataFrame of schema { word: String, timestamp: Timestamp }
    val wordsTime=words.select($"word",unix_timestamp($"time","yyyy/MM/dd HH:mm:ss").cast(TimestampType).as("timestamp"))

    val windowedCounts=words.withWatermark("timestamp","10 minutes")
      .groupBy(window($"timestamp","10 minutes","5 minutes"), $"word").count()

    /* In this example, we are defining the watermark of the query on the value of the column “timestamp”,
    * and also defining “10 minutes” as the threshold of how late is the data allowed to be. If this query is run in
    * Update output mode (discussed later in Output Modes section), the engine will keep updating counts of a window
    * in the Result Table until the window is older than the watermark, which lags behind the current event time in
    * column “timestamp” by 10 minutes. */

    val query=windowedCounts.writeStream.format("console").outputMode("update").start()
    query.awaitTermination()
  }

  case class WordWithTime(word:String,time:String)

  def JoinOperationExample(spark:SparkSession):Unit={
    import spark.implicits._
    val schema=StructType(Array(
      StructField("device",StringType,false),
      StructField("deviceType",StringType,false),
      StructField("signal",DoubleType,false),
      StructField("time",StringType,false)
    ))
    val path="/DATA/data_set/spark/basics/Lesson6_Spark_Streaming/structrued_streaming/device"
    val deviceDf:DataFrame=spark.readStream.option("sep",";").schema(schema).csv(path)
    val providerDf:DataFrame=spark.sparkContext.parallelize(Seq(
      DeviceProvider("t1","pengfei","CN"),
      DeviceProvider("t2","HuaWei","CN"),
      DeviceProvider("t3","bar","FR")
    )).toDF()

    //stream df join static df, only inner and left join is supported,
    // The full supported join matrix is in section 6.5.5.1.1
    // inner equi-join with a static DF
    deviceDf.join(providerDf,"type")
  }

  case class DeviceProvider(device:String,deviceProvider:String,providerContry:String)
}
