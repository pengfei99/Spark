package org.pengfei.Lesson06_Spark_Streaming

import org.apache.spark.sql.SparkSession

object Lesson06_5_6_Structure_Streaming_Kafka_Integrations {

  def main(args:Array[String])={

  }

  /*****************************************************************************************************************
    * ***************************** 6.5.6 Spark Structure Streaming Kafka Integration *******************************
    * ***************************************************************************************************************/
  /* To use kafka input source in structured streaming, you need to add the following dependencies in your pom.xml(maven)
      * groupId = org.apache.spark
      * artifactId = spark-sql-kafka-0-10_2.11
      * version = 2.3.1
      *
      * You can find the official doc here :
      * https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html
      * */

  /********************************** 6.5.6.1 Kafka source for streaming queries ******************************/
  def StreamingQueryExample(spark:SparkSession):Unit={

    MultipleTopicsExample(spark)
    //
    PatternOfTopicsExample(spark)
  }

  /*****************************subscribe to multiple topics **************************************/
  def MultipleTopicsExample(spark:SparkSession):Unit={

    // create a stream dataframe which subscribe to two topics "news" and "movies"of kafka stream on server
    // host1/2 and port1/2.
    val df=spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers","host1:port1,host2:port2")
      .option("subscribe","news,movies")
      .load()

    /*df.selectExpr("CAST(key AS STRING)","CAST(value AS STRING)").as[(String,String)]*/
  }

  /***************************subscribe to pattern of topics **********************************/
def PatternOfTopicsExample(spark:SparkSession):Unit={
  val df=spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers","host1:port1,host2:port2")
    .option("subscribePattern","topic.*")
    .load()
}

  /********************************** 6.5.6.2 Kafka source for batch queries ******************************/

  def BatchQueryExample(spark:SparkSession):Unit={
    // OneTopicDefaultOffSet(spark)

    MultTopicWithOffSet(spark)

    PatternTopicWithOffSet(spark)

  }

  /*************************** subscribe to 1 topic defaults to the earliest and latest offsets ******************/
  def OneTopicDefaultOffSet(spark:SparkSession):Unit={
    //By default, if we don't specify the start/ending offset, spark will take the earliest to the latest
    val df = spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
      .option("subscribe", "topic1")
      .load()
   /* df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]*/
  }
  /*********************** subscribe to multiple topics, specifying explicit kafka offsets *************************/
  def MultTopicWithOffSet(spark:SparkSession):Unit={
    // Subscribe to multiple topics, specifying explicit Kafka offsets
    val df = spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
      .option("subscribe", "topic1,topic2")
      .option("startingOffsets", """{"topic1":{"0":23,"1":-2},"topic2":{"0":-2}}""")
      .option("endingOffsets", """{"topic1":{"0":50,"1":-1},"topic2":{"0":-1}}""")
      .load()
    /*df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]*/
  }

  /********************** subscribe to a pattern topics, at the earliest and latest offsets ************************/
  def PatternTopicWithOffSet(spark:SparkSession):Unit={
    // Subscribe to a pattern, at the earliest and latest offsets
    val df = spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
      .option("subscribePattern", "topic.*")
      .option("startingOffsets", "earliest")
      .option("endingOffsets", "latest")
      .load()
    /*df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]*/
  }

  /* Each row in the source has the following schema:
      - Column	Type
      - key	binary
      - value	binary
      - topic	string
      - partition	int
      - offset	long
      - timestamp	long
      - timestampType	int
 */

  /* The following options must be set for the Kafka source for both batch and streaming queries.
  *
Option  |             	value	           |          meaning
assign	| json string {"topicA":[0,1],"topicB":[2,4]}  |	Specific TopicPartitions to consume. Only one of "assign", "subscribe" or "subscribePattern" options can be specified for Kafka source.
subscribe	| A comma-separated list of topics	| The topic list to subscribe. Only one of "assign", "subscribe" or "subscribePattern" options can be specified for Kafka source.
subscribePattern |	Java regex string	| The pattern used to subscribe to topic(s). Only one of "assign, "subscribe" or "subscribePattern" options can be specified for Kafka source.
kafka.bootstrap.servers	| A comma-separated list of host:port	| The Kafka "bootstrap.servers" configuration.

*/

  /*****************************************************************************************************************
    * ***************************** 6.5.6.3 Writing data to kafka *************************************************
    * ***************************************************************************************************************/

  /* We write both streaming query and batch query to Kafka. Take note that Apache Kafka only supports at least once
  * write semantics. Consequently, when writing—either Streaming Queries or Batch Queries—to Kafka, some records may
  * be duplicated;
  *
  * This can happen, for example, if Kafka needs to retry a message that was not acknowledged by a Broker, even though
  * that Broker received and wrote the message record. Structured Streaming cannot prevent such duplicates from
  * occurring due to these Kafka write semantics. However, if writing the query is successful, then you can assume
  * that the query output was written at least once. A possible solution to remove duplicates when reading the written
  * data could be to introduce a primary (unique) key that can be used to perform de-duplication when reading.
  *
  * The Dataframe being written to Kafka should have the following columns in schema:
  * Column	|  Type
  * key (optional) |	string or binary
  * value (required) |	string or binary
  * topic (*optional)	| string
  * The topic column is required if the “topic” configuration option is not specified in the writeStream.
  * The value column is the only required option. If a key column is not specified then a null valued key column
  * will be automatically added (see Kafka semantics on how null valued key values are handled). If a topic column
  * exists then its value is used as the topic when writing the given row to Kafka, unless the “topic” configuration
  * option is set i.e., the “topic” configuration option overrides the topic column.
*/

  /**************************************6.5.6.3.1 Write Stream Query to Kafka **************************/
  def WriteStreamQueryToKafkaExample(spark:SparkSession):Unit={
    TopicInDFExample(spark)
  }

  def TopicInDFExample(spark:SparkSession):Unit={
    import spark.implicits._
    val df=spark.sparkContext.parallelize(List(
      kafkaMessage("user","name","haha"),
      kafkaMessage("user","age","32"),
      kafkaMessage("user","name","foo"),
      kafkaMessage("user","age","22")
    )).toDF()
    // Write key-value data from a DataFrame to Kafka using a topic specified in the data
    val ds = df
      .selectExpr("topic", "CAST(key AS STRING)", "CAST(value AS STRING)")
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
      .start()
  }

  def TopicInOptionExample(spark:SparkSession):Unit={
    import spark.implicits._
    val df=spark.sparkContext.parallelize(List(
      Message("name","haha"),
      Message("age","32"),
      Message("name","foo"),
      Message("age","22")
    )).toDF()
    // Write key-value data from a DataFrame to a specific Kafka topic specified in an option
    val ds = df
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
      .option("topic", "user")
      .start()
  }

  /************************************** 6.5.6.3.2 Write Batch Query to Kafka *********************************/
  def WriteBatchQueryToKafkaExample(spark:SparkSession):Unit={

  }

  def BatchTopicInOption(spark:SparkSession):Unit={
    import spark.implicits._
    val df=spark.sparkContext.parallelize(List(
      Message("name","haha"),
      Message("age","32"),
      Message("name","foo"),
      Message("age","22")
    )).toDF()
    // Write key-value data from a DataFrame to a specific Kafka topic specified in an option
    df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
      .option("topic", "topic1")
      .save()
  }

  def BatchTopicInDF(spark:SparkSession):Unit={
    import spark.implicits._
    val df=spark.sparkContext.parallelize(List(
      kafkaMessage("user","name","haha"),
      kafkaMessage("user","age","32"),
      kafkaMessage("user","name","foo"),
      kafkaMessage("user","age","22")
    )).toDF()
    // Write key-value data from a DataFrame to Kafka using a topic specified in the data
    df.selectExpr("topic", "CAST(key AS STRING)", "CAST(value AS STRING)")
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
      .save()
  }

  case class kafkaMessage(topic:String,key:String,value:String)
  case class Message(key:String,value:String)

  /*****************************************************************************************************************
    * ***************************** 6.5.6.4 kafka Specific configurations *************************************************
    * ***************************************************************************************************************/

  /*
  * Kafka’s own configurations can be set via DataStreamReader.option with kafka. prefix, e.g,
  * stream.option("kafka.bootstrap.servers", "host:port"). For possible kafka parameters, see Kafka consumer config
  * docs for parameters related to reading data, and Kafka producer config docs for parameters related to writing data.
  *
  * Note that the following Kafka params cannot be set and the Kafka source or sink will throw an exception:
  * - group.id: Kafka source will create a unique group id for each query automatically.
  *
  * - auto.offset.reset: Set the source option startingOffsets to specify where to start instead. Structured Streaming
  *                      manages which offsets are consumed internally, rather than rely on the kafka Consumer to do it.
  *                      This will ensure that no data is missed when new topics/partitions are dynamically subscribed.
  *                      Note that startingOffsets only applies when a new streaming query is started, and that
  *                      resuming will always pick up from where the query left off.
  *
  * - key.deserializer: Keys are always deserialized as byte arrays with ByteArrayDeserializer. Use DataFrame
  *                     operations to explicitly deserialize the keys.
  *
  * - value.deserializer: Values are always deserialized as byte arrays with ByteArrayDeserializer. Use DataFrame
  *                      operations to explicitly deserialize the values.
  *
  * - key.serializer: Keys are always serialized with ByteArraySerializer or StringSerializer. Use DataFrame
  *                   operations to explicitly serialize the keys into either strings or byte arrays.
  *
  * - value.serializer: values are always serialized with ByteArraySerializer or StringSerializer. Use DataFrame
  *                   operations to explicitly serialize the values into either strings or byte arrays.
  *
  * - enable.auto.commit: Kafka source doesn’t commit any offset.
  *
  * - interceptor.classes: Kafka source always read keys and values as byte arrays. It’s not safe to use
  *                        ConsumerInterceptor as it may break the query.
  *
  *                        */

  /*****************************************************************************************************************
    * ****************************************** 6.5.6.5 Deploying *************************************************
    * ***************************************************************************************************************/

  /* As with any Spark applications, spark-submit is used to launch your application. spark-sql-kafka-0-10_2.11 and
   * its dependencies can be directly added to spark-submit using --packages, such as,
   *
   * ./bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.1 ...
   *
   * For experimenting on spark-shell, you can also use --packages to add spark-sql-kafka-0-10_2.11 and its
   * dependencies directly,
   *
   * ./bin/spark-shell --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.1 ...
   *
   * See Application Submission Guide for more details about submitting applications with external dependencies.*/

}
