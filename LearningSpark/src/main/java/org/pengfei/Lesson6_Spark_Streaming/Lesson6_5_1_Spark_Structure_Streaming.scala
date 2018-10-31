package org.pengfei.Lesson6_Spark_Streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

object Lesson6_5_1_Spark_Structure_Streaming {
/*****************************************************************************************************************
  * ********************************** 6.5.1 Structure Streaming *************************************************
  * ***************************************************************************************************************/

  /*
  * Spark official doc: https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#programming-model
  * Structured Streaming is a scalable and fault-tolerant stream processing engine built on the ""Spark SQL engine"".
  * You can express your streaming computation the same way you would express a batch computation on static data.
  * The Spark SQL engine will take care of running it incrementally and continuously and updating the final result
  * as streaming data continues to arrive. You can use the Dataset/DataFrame API in Scala, Java, Python or R to
  * express streaming aggregations, event-time windows, stream-to-batch joins, etc. The computation is executed
  * on the same optimized Spark SQL engine. Finally, the system ensures end-to-end exactly-once fault-tolerance
  * guarantees through checkpointing and Write Ahead Logs. In short, Structured Streaming provides fast, scalable,
  * fault-tolerant, end-to-end exactly-once stream processing without the user having to reason about streaming.
  *
  * Internally, by default, Structured Streaming queries are processed using a micro-batch processing engine, which
  * processes data streams as a series of small batch jobs thereby achieving end-to-end latencies as low as
  * 100 milliseconds and exactly-once fault-tolerance guarantees. However, since Spark 2.3, we have introduced
  * a new low-latency processing mode called Continuous Processing, which can achieve end-to-end latencies as low as
  * 1 millisecond with at-least-once guarantees. Without changing the Dataset/DataFrame operations in your queries,
  * you will be able to choose the mode based on your application requirements.
  *
  * In this guide, we are going to walk you through the programming model and the APIs. We are going to explain
  * the concepts mostly using the default micro-batch processing model, and then later discuss Continuous Processing
  * model. */

  /**/
  def main(args:Array[String])={
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val spark=SparkSession.builder.appName("Lesson6_5_Spark_Structure_Streaming").master("local[2]").getOrCreate()
    /********************************************First Example********************************/
   // FirstExample(spark)
    /*******************************************Input source Example ***********************************/
    FileInputSourceExample(spark)
  }


  /*****************************************************************************************************************
    * ********************************** 6.5.2 First Example *************************************************
    * ***************************************************************************************************************/

  def FirstExample(spark:SparkSession):Unit={
    /*
    * Let’s say you want to maintain a running word count of text data received from a data server listening on a TCP
    * socket. Let’s see how you can express this using Structured Streaming. First, we have to import the necessary
    * classes and create a local SparkSession, the starting point of all functionalities related to Spark.
    * */
    import spark.implicits._
    val host="localhost"
    val port=9999
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
    /* This lines DataFrame represents an unbounded table containing the streaming text data. This table contains
    * one column of strings named “value”, and each line in the streaming text data becomes a row in the table. Note,
    * that this is not currently receiving any data as we are just setting up the transformation, and have not yet
    * started it. flatMap operation to split each line into multiple words. The resultant words Dataset contains all the words.
    * Finally, we have defined the wordCounts DataFrame by grouping by the unique values in the Dataset and counting
    * them. Note that this is a streaming DataFrame which represents the running word counts of the stream.
    * */

    /* We have the data streaming data processing logic, Now we need to start receiving data and use the above logic.
     * The below code shows us how to print the complete set of counts (outputMode("complete")) to the console every
     * time they are updated. And the start method start the streaming computation*/

  val query=wordCounts.writeStream.outputMode("complete").format("console").start()

    query.awaitTermination()
  }

  /*****************************************************************************************************************
    * ********************************** 6.5.3 Structure Streaming Concept *****************************************
    * ***************************************************************************************************************/
/* The key idea in Structured Streaming is to treat a live data stream as a table that is being continuously appended.
* This leads to a new stream processing model that is very similar to a batch processing model. You will express your
* streaming computation as standard batch-like query as on a static table, and Spark runs it as an incremental query
* on the unbounded input table. Let’s understand this model in more detail.*/

  /*****************************************6.5.3.1 Basic Concepts************************************************/

  /* Consider the input data stream as the "Input table". Every data item that is arriving on the stream is like a
  * new row being appended to the input table.
  *
  * A query on the input will generate the "Result Table". Every trigger interval (for example, every 1 second),
  * new rows get appended to the input table. Which eventually updates the Result table. Whenever the result table
  * gets updated, we would want to write the changed result rows to an example sink.
  *
  * The "output" can have 3 different mode:
  * - Complete Mode : The entire updated Result Table will be written to the external storage. It is up to the storage
  *                   connector to decide how to handle writing of the entire table. Often used in query with
  *                   aggregation computation.
  * - Append Mode (default) : Only the new rows appended in the Result Table since the last trigger will be written to the
  *                 external storage. This is applicable only on the queries where existing rows in the Result Table
  *                 are not expected to change. Hence this mode guarantees that each row will be output only once
  *                 (assuming fault-tolerant sink)
  * - Update Mode : Only the rows that we updated in the Result Table since the last trigger will be written to the
  *                 external storage (available since Spark 2.1.1). Note that this is different from the complete Mode
  *                 in that this mode only outputs the rows that have changed since the last trigger. If the query
  *                 does not contain aggregations. It will be equivalent to Append mode.
  *
  * Note that Structured Streaming does not materialize the entire table. It reads the latest available data from
  * the streaming data source, processes it incrementally to update the result, and then discards the source data.
  * It only keeps around the minimal intermediate state data as required to update the result
  * (e.g. intermediate counts in the earlier example).
  *
  * This model is significantly different from many other stream processing engines. Many streaming systems require
  * the user to maintain running aggregations themselves, thus having to reason about fault-tolerance, and data
  * consistency (at-least-once, or at-most-once, or exactly-once). In this model, Spark is responsible for updating
  * the Result Table when there is new data, thus relieving the users from reasoning about it.
  *
  *
  * Different types of streaming queries support different output modes. Here is the compatibility matrix.
  *
  * */

  /******************************** 6.5.3.2 Handling Event-time and Late Data ****************************************/
  /* Event-time is the time embedded in the data itself. For many applications, you may want to operate on this
  * event-time. For example, if you want to get the number of events generated by IoT devices every minute, then you
  * probably want to use the time when the data was generated (that is, event-time in the data), rather than the time
  * Spark receives them. This event-time is very naturally expressed in this model – each event from the devices is
  * a row in the table, and event-time is a column value in the row. This allows window-based aggregations
  * (e.g. number of events every minute) to be just a special type of grouping and aggregation on the event-time column
  * – each time window is a group and each row can belong to multiple windows/groups. Therefore, such
  * event-time-window-based aggregation queries can be defined consistently on both a static dataset
  * (e.g. from collected device events logs) as well as on a data stream, making the life of the user much easier.
  *
  * Furthermore, this model naturally handles data that has arrived later than expected based on its event-time.
  * Since Spark is updating the Result Table, it has full control over updating old aggregates when there is late data,
  * as well as cleaning up old aggregates to limit the size of intermediate state data. Since Spark 2.1, we have support
  * for watermarking which allows the user to specify the threshold of late data, and allows the engine to accordingly
  * clean up old state. These are explained later in more detail in the Window Operations section.*/

  /******************************** 6.5.3.3 Fault Tolerance Semantics ****************************************/

  /* Delivering end-to-end exactly-once semantics was one of key goals behind the design of Structured Streaming.
  * To achieve that, we have designed:
  * - Structured Streaming sources
  * - sinks
  * - execution engine
  * to reliably track the exact progress of the processing so that it can handle any kind of failure by restarting
  * and/or reprocessing. Every streaming source is assumed to have offsets (similar to Kafka offsets, or Kinesis
  * sequence numbers) to track the read position in the stream. The engine uses checkpointing and write ahead logs
  * to record the offset range of the data being processed in each trigger. The streaming sinks are designed to be
  * idempotent for handling reprocessing. Together, using replayable sources and idempotent sinks, Structured Streaming
  * can ensure end-to-end exactly-once semantics under any failure.*/

  /***************************************** 6.5.3.4 Data types ********************************************/

  /* Since Spark 2.0, DataFrames and Datasets can represent static, bounded data, as well as streaming, unbounded data.
  * Similar to static Datasets/DataFrames, you can use the common entry point SparkSession to create streaming
  * DataFrames/Datasets from streaming sources, and apply the same operations on them as static DataFrames/Datasets.
  * If you are not familiar with Datasets/DataFrames, you can revisit Lesson4 spark sql*/


  /*****************************************************************************************************************
    * ***************************** 6.5.4 Creating streaming DataFrames and DataSets *******************************
    * ***************************************************************************************************************/

  /* Streaming DataFrames can be created through the DataStreamReader interface (Scala/Java/Python) returned by
  * SparkSession.readStream(). In R, with the read.stream() method. Similar to the read interface for creating
  * static DataFrame, you can specify the details of the source – data format, schema, options, etc.*/

  /***************************** 6.5.4.1 Input Sources **********************************************/
/* There a few built-in sources :
* - File source : Reads files written in a directory as a stream of dat. Supported file formats are text, csv, json,
*                 orc, parquet. See the following link for more details :
*                 https://spark.apache.org/docs/2.3.1/api/java/org/apache/spark/sql/streaming/DataStreamReader.html
*                 Note that the files must be atomically placed in the given directory, which in most file systems,
*                 can be achieved by the file move operations.
*
* - Kafka source : Reads data from Kafka. It's compatible with Kafka broker version 0.10.0 or higher. See the Kafka
*                 integration doc (https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html)
*
* - Socket source (For testing): Reads UTF8 text data from a socket connection. The listening server socket is at the
*                                driver. Note that this should be used only for testing as this does not provide
*                                end-to-end fault-tolerance guarantees.
*
* - Rate source (for testing) - Generates data at the specified number of rows per second, each output row contains a
*                               timestamp and value. Where timestamp is a Timestamp type containing the time of message
*                               dispatch, and value is of Long type containing the message count, starting from 0 as
*                               the first row. This source is intended for testing and benchmarking.
*
* Note that socket source is not fault-tolerant, because they do not guarantee that data can be replayed using
* checkpointed offsets after a failure.
* */

  def FileInputSourceExample(spark:SparkSession):Unit={
  /* Note that file input source works like the streaming context file source. The update of file content will not be
  * picked up by the stream. Only the creating of new file can be picked up by the stream.*/

/**************************************Csv file inut source *********************************************/
  CSVInputSourceExample(spark)

}
  /* This example read csv files, and return a df which contains user older than 23. */
  def CSVInputSourceExample(spark:SparkSession):Unit={
    val path="/DATA/data_set/spark/basics/Lesson6_Spark_Streaming/structrued_streaming"
    val userSchema = new StructType().add("name","string").add("age","integer")
    val userDf=spark.readStream.option("sep",";").schema(userSchema).csv(path)
    val oldDf=userDf.filter(userDf("age")>23)
    val query=oldDf.writeStream.outputMode("update").format("console").start()

    query.awaitTermination()
  }


  /*************************************Kafka input source ***********************************************/
  /* See the section 6.5.6 Kafka integration*/



  /****************** 6.5.4.2 Schema inference and partition of streaming DataFrames/DataSets **********************/

/* By default, Structured Streaming from file based sources requires you to specify the schema, rather than rely on
* Spark to infer it automatically. This restriction ensures a consistent schema will be used for the streaming query,
* even in the case of failures. For ad-hoc use cases, you can re-enable schema inference by setting
* spark.sql.streaming.schemaInference to true.
*
* Partition discovery does occur when subdirectories that are named /key=value/ are present and listing will
* automatically recurse into these directories. If these columns appear in the user provided schema, they will be
* filled in by Spark based on the path of the file being read. The directories that make up the partitioning scheme
* must be present when the query starts and must remain static. For example, it is okay to add /data/year=2016/ when
* /data/year=2015/ was present, but it is invalid to change the partitioning column (i.e. by creating the
* directory /data/date=2016-04-17/).
* */



}
