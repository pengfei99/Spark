package org.pengfei.spark.streaming.flume

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.streaming.flume._
import org.apache.spark.util.IntParam
object FlumeEventCount {
  def main(args: Array[String]) {

    //StreamingExamples.setStreamingLogLevels()
    val host="127.0.0.1"
    val port=6666
    val batchInterval = Milliseconds(2000)
    // Create the context and set the batch size
    val sparkConf = new SparkConf().setAppName("FlumeEventCount").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, batchInterval)
    // Create a flume stream
    val stream = FlumeUtils.createStream(ssc, host, port, StorageLevel.MEMORY_ONLY_SER_2)
    // Print out the count of events received from this server in each batch

    stream.count().map(cnt => "Received " + cnt + " flume events." ).print()

    val mappedlines = stream.map{sparkFlumeEvent =>
      val event = sparkFlumeEvent.event
      println("Value of event " + event)
      println("Value of event Header " + event.getHeaders)
      println("Value of event Schema " + event.getSchema)
      val messageBody = new String(event.getBody.array())
      println("Value of event Body " + messageBody)
      messageBody}.print()
    ssc.start()
    ssc.awaitTermination()
  }
}