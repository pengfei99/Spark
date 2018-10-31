package org.pengfei.Lesson6_Spark_Streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.functions.current_timestamp
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Lesson6_3_Processing_Data_Stream {

  /******************************************************************************************************
    * *****************************************6.3 Processing Data Stream *********************************
    * ******************************************************************************************************/

  /*
  * An application processes a data stream using the methods defined in the DStream and related classes.
  * DStream supports two types of operations: transformation and output operation. The transformations can
  * be further classified into
  * - basic
  * - aggregation
  * - key-value
  * - special transformation.
  *
  * Similar to RDD transformations, DStream transformations are lazily computed. No computation takes
  * places immediately when a transformation operation is called. An output operation triggers the execution of
  * DStream transformation operations. In the absence of an output operation on a DStream, Spark Streaming
  * will not do any processing, even if transformations are called on that DStream.*/

  def main(args:Array[String])={
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val master="local[2]"
    val appName="Lesson6_3_Processing_Data_Stream"
    val spark=SparkSession.builder().appName(appName).master(master).getOrCreate()

    /*****************************************Basic Transformation**********************************************/
    //BasicTransformationExample(spark)

    /******************************************Aggregation Transformation***************************************/
    //AggregationTransformationExample(spark)

    /*******************************************Key value transformation ***************************************/
   // KeyValueTransformationExample(spark)

    /*******************************************Special transformation ******************************************/
   // SpecialTransformationExample(spark)

    /*********************************************output operation**************************************************/
    // OutputOperationsExample(spark)
    /*********************************************Window operation *************************************************/
  WindowOperationExample(spark)

  }

  /******************************************************************************************************
    * *****************************************6.3.1 Basic Transformation *********************************
    * ******************************************************************************************************/

  def BasicTransformationExample(spark:SparkSession):Unit={
    val batchInterval=10
    val ssc=new StreamingContext(spark.sparkContext,Seconds(batchInterval))
    val host="localhost"
    val port=9999

    val checkPointPath="/tmp/spark/check-point"
    ssc.checkpoint(checkPointPath)

    val dStreams=ssc.socketTextStream(host,port)
  /*
  * A transformation applies a user-defined function to each element in a DStream and returns a new DStream.
  * DStream transformations are similar to RDD transformations. In fact, Spark Streaming converts a DStream
  * transformation method call to a transformation method call on the underlying RDDs. Spark core computes
  * the RDD transformations.*/

    /**********************************************Map/flatMap****************************************************/
    /* The map method takes a function as argument and applies it to each element in the source DStream to create
     * a new DStream. It returns a new DStream.
     *
     * They works like the map/flatMap of RDDs*/


    val mapToWords=dStreams.map(_.split(" "))
    mapToWords.print()
    val flatMapToWords=dStreams.flatMap(_.split(" "))
    flatMapToWords.print()

    /******************************************filter***********************************************************/
    /* The filter method returns a new DStream created by selecting only those element in the source DStream
     * for which the user-provided input function returns true.*/
    val containsWords=dStreams.filter(_.contains("them"))
    containsWords.print()

/*********************************************repartition****************************************************/
   /* The repartition method returns a new DStream in which each RDD has the specified number of partitions.
   * It allows you to distribute input data stream across a number of machines for processing. It is used to change
   * the level of processing parallelism. More partitions increase parallelism, while fewer partitions reduce
   * parallelism.*/
  dStreams.repartition(6)

   /**************************************** Union ****************************************************/
   /* The union method returns a new DStream that contains the union of the elements in the source DStream and
    * the DStream provided as input to this method*/

    /* val steam1 = ...
    *  val steam2 = ...
    *  val combinedStream = stream1.union(stream2)*/

    ssc.start()
    ssc.awaitTermination()
  }

  /***********************************************************************************************************
    * *****************************************6.3.2 Aggregation Transformation *********************************
    * ******************************************************************************************************/

def AggregationTransformationExample(spark:SparkSession)={
  val batchInterval=10
  val ssc=new StreamingContext(spark.sparkContext,Seconds(batchInterval))
  val host="localhost"
  val port=9999

  val checkPointPath="/tmp/spark/check-point"
  ssc.checkpoint(checkPointPath)

  val dStreams=ssc.socketTextStream(host,port)
  /*********************************************count*******************************************************/
  /* The count method returns a DStream of single-element RDDs. Each RDD in the returned DStream has the
   * count of the elements in the corresponding RDD in the source DStream. */
  val countsPerRdd=dStreams.count()

  //dStream must have a action such as print to start the transformation
  countsPerRdd.print()

  //The println only execute once. so we must use print method of the dstream to see the result of each steam(micro batch)
  println(s"countsPerRdd value ${countsPerRdd}")

  /*********************************************reduce******************************************************/

  /* The reduce method returns a DStream of single-element RDDs by reducing the elements in each RDD in the
   * source DStream. It takes a user provided reduce function as an argument.*/

  val words = dStreams.flatMap(_.split(" "))
  val longWord=words.reduce((w1,w2)=> if(w1.length>w2.length) w1 else w2)
  longWord.print()

  /****************************************countByValue**********************************************************/

  /* The countByValue method returns a DStream of key-value pairs, where a key in a pair is a distinct element
   * within a batch interval and the value is its count. Thus, each RDD in the returned DStream contains the
   * count of each distinct element in the corresponding RDD in the source DStream.*/

  val wordCounts=words.countByValue()
  wordCounts.print()




  ssc.start()
  ssc.awaitTermination()
}

  /***********************************************************************************************************
    * *****************************************6.3.3 Key value Transformation *********************************
    * ******************************************************************************************************/
def KeyValueTransformationExample(spark:SparkSession)={
  val batchInterval=10
  val ssc=new StreamingContext(spark.sparkContext,Seconds(batchInterval))

  val host="localhost"
  val port=9999

  val checkPointPath="/tmp/spark/check-point"
  ssc.checkpoint(checkPointPath)

  val dStreams=ssc.socketTextStream(host,port)
  /* DStreams of key-value pairs support a few other transformations in addition to the transformations available
   * on all types of DStreams.*/

  /****************************************************cogroup**************************************************/

  /* The cogroup method returns a DStream of (K, Seq[V1], Seq[V2]) when called on a DStream of (K, Seq[V1]) and
   * (K, Seq[V2]) pairs. It applies a cogroup operation between RDDs of the DStream passed as argument and
   * those in the source DStream.
   *
   * The following example used cogroup method to find the words with the same length from two DStreams*/

  /*val lines1 = ssc.socketTextStream("localhost", 9999)
val words1 = lines1 flatMap {line => line.split(" ")}
val wordLenPairs1 = words1 map {w => (w.length, w)}
val wordsByLen1 = wordLenPairs1.groupByKey
val lines2 = ssc.socketTextStream("localhost", 9998)
val words2 = lines2 flatMap {line => line.split(" ")}
val wordLenPairs2 = words2 map {w => (w.length, w)}
val wordsByLen2 = wordLenPairs2.groupByKey

val wordsGroupedByLen = wordsByLen1.cogroup(wordsByLen2)
*/


  /***********************************************Join***********************************************************/

  /* The join method takes a DStream of key-value pairs as argument and returns a DStream, which is an inner
   * join of the source DStream and the DStream provided as input. It returns a DStream of (K, (V1, V2)) when
   * called on DStreams of (K, V1) and (K, V2) pairs.
   *
   * The following example creates two DStreams of lines of text. It then splits them into DStreams of words. Next, it
* creates DStreams of key-value pairs, where a key is the length of a word and value is the word itself. Finally, it
* joins those two DStreams.*/

  /*
  * val lines1 = ssc.socketTextStream("localhost", 9999)
val words1 = lines1 flatMap {line => line.split(" ")}
val wordLenPairs1 = words1 map {w => (w.length, w)}
val lines2 = ssc.socketTextStream("localhost", 9998)
val words2 = lines2 flatMap {line => line.split(" ")}
val wordLenPairs2 = words2 map {w => (w.length, w)}
val wordsSameLength = wordLenPairs1.join(wordLenPairs2)

*/

  /* Left outer, right outer, and full outer join operations are also available. If a DStream of key value pairs
* of type (K, V) is joined with another DStream of pairs of type (K, W), full outer join returns a DStream of
* (K, (Option[V], Option[W])), left outer join returns a DStream of (K, (V, Option[W])), and righter outer join
* returns a DStream of (K, (Option[V], W)).
* */

  /*
  * val leftOuterJoinDS = wordLenPairs1.leftOuterJoin(wordLenPairs2)
val rightOuterJoinDS = wordLenPairs1.rightOuterJoin(wordLenPairs2)
val fullOuterJoinDS = wordLenPairs1.fullOuterJoin(wordLenPairs2)*/

  /**************************************************groupByKey**************************************************/
  /* The groupByKey method groups elements within each RDD of a DStream by their keys. It returns a new
   * DStream by applying groupByKey to each RDD in the source DStream.*/

  val words=dStreams.flatMap(line=>line.split(" "))
  val wordLenPairs= words.map(word=>(word.length,word))
  val wordsByLen=wordLenPairs.groupByKey()
  wordsByLen.print()

  /**************************************************reduceByKey**************************************************/
  /* The reduceByKey method returns a new DStream of key-value pairs, where the value for each key is obtained by
   * applying a user-provided reduce function on all the values for that key within an RDD in the source DStream.
   * The following example counts the number of times a word occurs within each DStream micro-batch.
   * */

  val wordPairs=words.map(w=>(w,1))
  val wordCounts = wordPairs.reduceByKey(_ + _)
  wordCounts.print()

  ssc.start()
  ssc.awaitTermination()
}
  /***********************************************************************************************************
    * *****************************************6.3.4 Special Transformation *********************************
    * ******************************************************************************************************/

  def SpecialTransformationExample(spark:SparkSession)={

    val batchInterval=10
    val ssc=new StreamingContext(spark.sparkContext,Seconds(batchInterval))

    val host="localhost"
    val port=9999

    val checkPointPath="/tmp/spark/check-point"
    ssc.checkpoint(checkPointPath)

    val dStream=ssc.socketTextStream(host,port)
    /* The transformations discussed above allow you to specify operations on the elements in a DStream. Under
     * the hood, DStream converts them to RDD operations. The next two transformations deviate from this model. */

    /*******************************************************Transform***********************************************/

    /* The transform method returns a DStream by applying an RDD => RDD function to each RDD in the source
* DStream. It takes as argument a function that takes an RDD as argument and returns an RDD. Thus, it gives
* you direct access to the underlying RDDs of a DStream.
*
* This method allows you to use methods provided by the RDD API, but which do not have equivalent
* operations in the DStream API. For example, sortBy is a transformation available in the RDD API, but not in
* the DStream API. If you want to sort the elements within each RDD of a DStream, you can use the transform
* method as shown in the following example.*/

    val words = dStream.flatMap{line => line.split(" ")}
    val sorted = words.transform{rdd => rdd.sortBy((w)=> w)}

    //sorted.print()

    /* The transform method is also useful for applying machine learning and graph computation algorithms to data
* streams. The machine learning and graph processing libraries provide classes and methods that operate at
* the RDD level. Within the transform method, you can use the API provided by these libraries.*/

    /***********************************************UpdateStateByKey**********************************************/

    /* The updateStateByKey method allows you to create and update states for each key in a DStream of key value
    * pairs. You can use this method to maintain any information about each distinct key in a DStream.
    *
    * For example, you can use the updateStateByKey method to keep a running count of each distinct word
    * in a DStream, as shown in the following example.*/

    val wordPairs = words.map{word=>(word,1)}

    val updateState=(xs:Seq[Int],prevState:Option[Int])=>{
      prevState match {
        case Some(prevCount)=>Some(prevCount+xs.sum)
        case None => Some(xs.sum)
      }
    }
    val runningCount = wordPairs.updateStateByKey(updateState)
    runningCount.print()

    /* The Spark Streaming library provides multiple overloaded variants of the updateStateByKey method.
    * The simplest version of the updateStateByKey method takes a function of type (Seq[V], Option[S]) => Option[S]
    * as an argument. This user-provided function takes two arguments. The first argument is a sequence of new
    * values for a key in a DStream RDD and the second argument is previous state of the key wrapped in the
    * Option data type. The user-provided function updates the state of a key using the new values and previous
    * state of a key, and returns the new state wrapped in the Option data type. If the update function returns None
    * for a key, Spark Streaming stops maintaining state for that key.
    *
    * The updateStateByKey method returns a DStream of key-value pairs, where the value in a pair is the
    * current state of the key in that pair.*/

    ssc.start()
    ssc.awaitTermination()
  }

  /***********************************************************************************************************
    * ***************************************** 6.3.5 Output Operations **************************************
    * ******************************************************************************************************/

  def OutputOperationsExample(spark:SparkSession):Unit={
    val batchInterval=10
    val ssc= new StreamingContext(spark.sparkContext,Seconds(batchInterval))

    val host="localhost"
    val port=9999

    val checkPointPath="/tmp/spark/check-point"
    ssc.checkpoint(checkPointPath)

    val dStream=ssc.socketTextStream(host,port)
    /* Output operations are DStream methods that can be used to send DStream data to an output destination.
     * An output destination can be a file, database, or another application. Output operations are executed
     * sequentially in the order in which they are called by an application.
     * */

    /**********************************************Saving to a File system*************************************/
    /* The commonly used DStream output operations for saving a DStream to file system */

    val outputDir="/tmp/spark/output"
    val objOutputDir="/tmp/spark/objOutput"

    val words=dStream.flatMap(_.split(" "))
    val wordPairs=words.map(w=>(w,1))
    val wordCounts=wordPairs.reduceByKey(_ + _)

    wordCounts.saveAsTextFiles(outputDir)


    /********************************************Saving as object Files **************************************/

    /* The saveAsObjectFiles method saves DStream elements as serialized objects in binary SequenceFiles.
     * Similar to the saveAsTextFile method, it stores the data for each DStream RDD in a separate directory
     * and creates a file for each RDD partition. The directory name for each DStream RDD is generated using the
     * current timestamp and a user-provided prefix and optional suffix. */

    // wordCounts.saveAsObjectFiles(objOutputDir)

    /********************************************save as hadoop files *******************************************/

    /* The saveAsHadoopFiles method is available on DStreams of key-value pairs. It saves each RDD in the source
    * DStream as a Hadoop file. */

    /********************************************save as new api hadoop files **************************************/
    /* Similar to the saveAsHadoopFiles method, the saveAsNewAPIHadoopFiles method saves each RDD in a
     * DStream of key-value pairs as a Hadoop file.*/

    /************************************************Displaying on Console****************************************/

    /* The DStream class provides the print method for displaying a DStream on the console of the machine where
     * the driver program is running.
     *
     * The print method, prints the elements in each RDD in the source DStream on the
     * machine running the driver program. By default, it shows the first ten elements in each RDD. An overloaded
     * version of this method allows you to specify the number of elements to print.
     * */

    /************************************************Saving into a DataBase ****************************************/

    /* The foreachRDD method in the DStream class can be used to save the results obtained from processing a
     * DStream into a database.
     *
     * The foreachRDD method is similar to the transform method discussed earlier. It gives you access to the
     * RDDs in a DStream. The key difference between transform and foreachRDD is that transform returns a new
     * DStream, whereas foreachRDD does not return anything.
     *
     * The foreachRDD method is a higher-order method that takes as argument a function of type RDD => Unit.
     * It applies this function to each RDD in the source DStream. All RDD operations are available to this function.
     * It is important to note that the foreachRDD method is executed on the driver node; however, the RDD
     * transformations and actions called within foreachRDD are executed on the worker nodes.
     *
     * Two things have to be kept in mind when saving a DStream into a database. First, creating a database
     * connection is an expensive operation. It is recommended not to open and close database connections
     * frequently. Ideally, you should re-use a database connection for storing as many elements as possible to
     * amortize the cost of creating a connection. Second, a database connection generally cannot be serialized
     * and sent from master to worker nodes. Since DStreams are processed on worker nodes, database
     * connections should be created on worker nodes.
     *
     * The RDD foreachPartition action can be used for storing multiple DStream elements using the same
     * database connection. Since the foreachRDD DStream method gives you access to all RDD operations, you
     * can call the foreachPartition RDD method within foreachRDD. Within foreachPartition, you can open a
     * database connection and use that connection to store all elements in the source RDD partition. You can further
     * optimize by using a connection pool library instead of opening and closing a physical connection directly.
     * The following code snippet implements the approach described earlier for saving a DStream to a
     * database. It assumes that the application is using a connection pool library such as HikariCP or BoneCP. The
     * connection pool library is wrapped in a lazily initialized singleton object named ConnectionPool, which
     * manages a pool of database connections.
     *
     * If you know the receiving data structure, you can also use rdd and dataframe api to save the data into the
     * database.
     * */
   // val structruredDStream=wordCounts.map{(word:String,count:Int)=>WordCounts(word,count)}
   /*

    wordCounts.foreachRDD{ rdd=>

      }

    }*/
/* Another optimization that you can do is batch the database writes. So instead of sending one database
* write per element, you can batch all the inserts for an RDD partition and send just one batch update to the
* database per RDD partition.
* The foreachRDD method comes handy for not only saving a DStream to a database, but it is also useful
* for displaying the elements in a DStream in a custom format on the driver node.*/
    ssc.start()
    ssc.awaitTermination()
  }


case class WordCounts(word:String,count:Int)

  /***********************************************************************************************************
    * ***************************************** 6.3.6 Window Operations **************************************
    * ******************************************************************************************************/

  def WindowOperationExample(spark:SparkSession):Unit={
    val batchInterval= 10
    val ssc=new StreamingContext(spark.sparkContext,Seconds(batchInterval))
    val host="localhost"
    val port=9999
    val checkPointPath="/tmp/spark/check-point"
    ssc.checkpoint(checkPointPath)

    val dStream=ssc.socketTextStream(host,port)
    /* A window operation is a DStream operation that is applied over a sliding window of data in a stream.
* Successive windows have one or more overlapping RDDs (see Figure 6-5 page 94). A window operation is a stateful
* DStream operation that combines data across multiple batches.
*
* A window operation requires two parameters (see Figure 6-6 page 94):
* - window length : specifies the time duration over which a window operation is applied.
* - sliding interval : specifies the time interval at which a window operation is performed. It is the time interval
*                     at which new RDDs are generated by a window operation.
*
* Important note : Both the window length and sliding interval parameters must be a multiple of a DStream’s
*                  batch interval.
*
* */
    /************************************* Window method *****************************************/
    /* The window method returns a DStream of sliding RDDs. It takes two arguments, window duration and sliding
     * interval. Each RDD in the returned DStream includes elements from the source DStream for the specified
     * duration and a new RDD is generated at the specified time interval. Successive RDDs in the returned
     * DStream have overlapping data elements.*/

   // WindowMethod(dStream)



/******************************************CountByWindow*******************************************/

    /* The countByWindow method returns a DStream of single-element RDDs. The single element in each
     * returned DStream RDD is the count of the elements in a sliding window of a specified duration. It takes two
     * arguments, window duration, and sliding interval.*/

    // CountByWindowMethod(dStream)


    /****************************************countByValueAndWindow*******************************/

    /* The countByValueAndWindow method returns a DStream containing the counts of each distinct element
     * within a sliding window that slides at the specified time interval.*/

   // CountByValueAndWindow(dStream)

    /**************************************** reduceByWindow ***************************************/
    /* The reduceByWindow method returns a DStream of single-element RDDs. Each RDD in the returned DStream
     * is generated by applying a user-provided reduce function over the DStream elements in a sliding window.
     * The reduceByWindow method takes three arguments: reduce function, window duration, and sliding interval.
     *
     * The user-provided reduce function must be of type (T, T) => T. It takes two arguments of type T and returns
     * a single value of type T. This function is applied to all the elements within a window to generate a
     * single value. It can be used to aggregate elements within each sliding window.*/

    // ReduceByWindowMethod(dStream)

    /******************************************reduceByKeyAndWindow*********************************/

    /* The reduceByKeyAndWindow operation is available only for DStreams of key-value pairs. It is similar to
     * reduceByWindow, except that it does the same thing for a DStream of key-value pairs. It applies a user provided
     * reduce function to key-value pairs in a sliding DStream window to generate single key-value pair
     * for each distinct key within a window.*/

    ReduceByKeyAndWindow(dStream)

    /******************************reduceByKeyAndWindow with invFunc************************************/
    /* In a windowing operation, each new window overlaps with previous window. It adds some elements
     * to and removes some from the previous window. For example, if the window duration is 60 seconds and
     * sliding interval is 10 seconds, each new window removes 10 seconds of data from previous window and adds
     * 10 seconds of new data. Successive windows share 40 seconds of data. Performing complete aggregation
     * over 60 seconds for every window is inefficient. A more efficient approach is to add the aggregate for the 10
     * seconds of new data to the previous window’s result and remove the aggregate for the 10 seconds of data that
     * is no longer in the new window.
     *
     * Spark Streaming provides an efficient variant of the reduceByKeyAndWindow operation, which
     * incrementally updates a sliding window by using the reduced value of the predecessor window. It requires
     * n additional inverse reduce function as an argument. It reduces the new values that enter a windows and
     * uses the inverse reduce function to remove the values that left the window.
     *
     * Note that the reduce function must have a corresponding "inverse reduce" function. - is the inverse function of +*/


    ssc.start()
    ssc.awaitTermination()
  }

  def WindowMethod(dStream:DStream[String]):Unit={
    val words:DStream[String] = dStream.flatMap(line=>line.split(" "))
    val windowLen=40
    val slidingInterval = 20
    val window = words.window(Seconds(windowLen),Seconds(slidingInterval))
    val longestWord = window.reduce{(word1,word2)=> if (word1.length > word2.length) word1 else word2}
    longestWord.print()
  }

  def CountByWindowMethod(dStream: DStream[String]):Unit={
    val words:DStream[String] = dStream.flatMap(line=>line.split(" "))
    val windowLen=40
    val slidingInterval = 20
    val countByWindow = words.countByWindow(Seconds(windowLen),Seconds(slidingInterval))
    countByWindow.print()
  }

  def CountByValueAndWindow(dStream: DStream[String]):Unit={
    val words:DStream[String] = dStream.flatMap(line=>line.split(" "))
    val windowLen=40
    val slidingInterval = 20

    val countByValueAndWindow = words.countByValueAndWindow(Seconds(windowLen),Seconds(slidingInterval))
    countByValueAndWindow.print()
  }

  def ReduceByWindowMethod(dStream: DStream[String]):Unit={
    val numbers=dStream.flatMap(_.split(" ")).map(x=>x.toInt)
    val windowLen=30
    val slidingInterval = 10

    val sumLast30Seconds = numbers.reduceByWindow({(n1,n2)=>n1+n2},Seconds(windowLen),Seconds(slidingInterval))
    sumLast30Seconds.print()
  }

  def ReduceByKeyAndWindow(dStream: DStream[String]):Unit={
    val words:DStream[String]=dStream.flatMap(line=>line.split(" "))
    val wordPair=words.map{word=>(word,1)}
    val windowLen=40
    val slidingInterval = 10

    val Last40SecondsWordCounts=wordPair.reduceByKeyAndWindow((count1:Int,count2:Int)=>count1+count2,
      Seconds(windowLen),Seconds(slidingInterval))

    Last40SecondsWordCounts.print()
  }

  def SmartReduceByKeyAndWindow(dStream: DStream[String]):Unit={
    val words = dStream flatMap {line => line.split(" ")}
    val wordPairs = words map {word => (word, 1)}
    val windowLen = 30
    val slidingInterval = 10

    def add(x: Int, y: Int): Int = x + y

    def sub(x: Int, y: Int): Int = x-y

    // val wordCountLast30Seconds = wordPairs.reduceByKeyAndWindow(add, sub, Seconds(windowLen), Seconds(slidingInterval))

    // wordCountLast30Seconds.print()


  }

  /***********************************************************************************************************
    * ***************************************** 6.3.7 Caching/persistence **************************************
    * ******************************************************************************************************/

  /* Similar to RDDs, DStreams also allow developers to persist the stream’s data in memory. That is, using the
  * persist() method on a DStream will automatically persist every RDD of that DStream in memory. This is useful
  * if the data in the DStream will be computed multiple times (e.g., multiple operations on the same data).
  * For window-based operations like reduceByWindow and reduceByKeyAndWindow and state-based operations
  * like updateStateByKey, this is implicitly true. Hence, DStreams generated by window-based operations
  * are automatically persisted in memory, without the developer calling persist().
  *
  * For input streams that receive data over the network (such as, Kafka, Flume, sockets, etc.), the default
  * persistence level is set to replicate the data to two nodes for fault-tolerance.
  *
  * Note that, unlike RDDs, the default persistence level of DStreams keeps the data serialized in memory.
  * This is further discussed in the Performance Tuning section. More information on different persistence levels
  * can be found in the Spark Programming Guide StorageLevel.
  * */

  /***********************************************************************************************************
    * ********************************** 6.3.8 Handling late data and Watermarking ****************************
    * ******************************************************************************************************/

  /*
  * You can find complete guide in https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
  * We have seen how to use window method to group dstream. Now consider what happens if one of the events arrives
  * late to the application. For example, say, a word generated at 12:04 (i.e. event time) could be received by
  * the application at 12:11. The application should use the time 12:04 instead of 12:11 to update the older
  * counts for the window 12:00 - 12:10. This occurs naturally in our window-based grouping – Structured Streaming
  * can maintain the intermediate state for partial aggregates for a long period of time such that late data can
  * update aggregates of old windows correctly, as illustrated below.
  *
  * However, to run this query for days, it’s necessary for the system to bound the amount of intermediate
  * in-memory state it accumulates. This means the system needs to know when an old aggregate can be dropped
  * from the in-memory state because the application is not going to receive late data for that aggregate any more.
  * To enable this, in Spark 2.1, we have introduced watermarking, which lets the engine automatically track the
  * current event time in the data and attempt to clean up old state accordingly. You can define the watermark of
  * a query by specifying the event time column and the threshold on how late the data is expected to be in terms
  * of event time. For a specific window starting at time T, the engine will maintain state and allow late data to
  * update the state until (max event time seen by the engine - late threshold > T). In other words, late data within
  * the threshold will be aggregated, but data later than the threshold will start getting dropped
  * (see later in the section for the exact guarantees). Let’s understand this with an example. We can easily
  * define watermarking on the previous example using withWatermark() as shown below.*/

  /*def WaterMarkExample(spark:SparkSession)={
    import spark.implicits._
    val batchInterval=10
    val ssc=new StreamingContext(spark.sparkContext,Seconds(batchInterval))
    val checkPointPath="/tmp/spark/check-point"
    ssc.checkpoint(checkPointPath)

    val host="localhost"
    val port=9999

    val dStream=ssc.socketTextStream(host,port)
    val words=dStream.flatMap(_.split(" "))
    val wordsStructured=words.map{w:String=>Words(w)}
    val wordDf=wordsStructured.foreachRDD(word=>word.toDF())
    val wordWithTimeDf=wordDf.withColumn("currentTimestamp",current_timestamp())

    // Group the data by window and word and compute the count of each group
    val windowedCounts = words
      .withWatermark("timestamp", "10 minutes")
      .groupBy(
        window($"timestamp", "10 minutes", "5 minutes"), $"word")
      .count()

  }*/
  /*def getCurrentTime():Timestamp={

  }*/
  case class Words(word:String)

}
