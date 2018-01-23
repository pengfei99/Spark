package org.pengfei.spark.streaming.kafka

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._

object KafkaWordCount {
  def main(args:Array[String]){
    //StreamingExamples.setStreamingLogLevels()
    val master = "spark://hadoop-nn.bioaster.org:7077"
    val sc = new SparkConf().setAppName("KafkaWordCount").setMaster(master)
    val ssc = new StreamingContext(sc,Seconds(10))
    //Set the context to periodically checkpoint the DStream operations for spark master fault-tolerance.
    //The checkpoint can be write on local file as in the example
    // It can be also write on hdfs,ssc.checkpoint("/user/hadoop/checkpoint")
    // make sure your hdfs is running
    ssc.checkpoint("file:///tmp/kafka/checkpoint")
    val zkQuorum = "hadoop-nn.bioaster.org:2181" //Zookeeper server url
    val group = "1"  //set topic group, for example val group = "test-consumer-group"
    val topics = "Hello-Kafka"  //topics name
    val numThreads = 3  //set topic partition number
    val topicMap =topics.split(",").map((_,numThreads.toInt)).toMap
    val lineMap = KafkaUtils.createStream(ssc,zkQuorum,group,topicMap)
    val lines = lineMap.map(_._2)
    val words = lines.flatMap(_.split(" "))
    val pair = words.map(x => (x,1))
    val wordCounts = pair.reduceByKeyAndWindow(_ + _,_ - _,Minutes(2),Seconds(10),2)
    wordCounts.print
    ssc.start
    ssc.awaitTermination
  }
}
