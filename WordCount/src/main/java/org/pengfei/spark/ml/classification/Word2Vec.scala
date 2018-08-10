package org.pengfei.spark.ml.classification

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.sql.SparkSession

object Word2Vec {
def main(args:Array[String]): Unit ={
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val spark = SparkSession.builder().
    master("local[2]"). //spark://10.70.3.48:7077 remote
    appName("Word2Vec").
    getOrCreate()

  import spark.implicits._

  val documentDF = spark.createDataFrame(Seq(
    "Hi I heard about Spark".split(" "),
    "I wish Java could use case classes".split(" "),
    "Logistic regression models are neat".split(" ")
  ).map(Tuple1.apply)).toDF("text")

  //documentDF.printSchema()
  /*
  * root
 |-- text: array (nullable = true)
 |    |-- element: string (containsNull = true)
  *
  * */
  //documentDF.show()

  /*+--------------------+
  |                text|
  +--------------------+
  |[Hi, I, heard, ab...|
  |[I, wish, Java, c...|
  |[Logistic, regres...|
  +--------------------+*/

/*
* Create an instance of word2Vec model, it's a estimator
* */
  //input col set the input column of the input dataset
  //output col set the output column of the result dataset
  //vectorSize set the size of generated word vec, in our case it's 6
  val word2Vec = new Word2Vec().
           setInputCol("text").
           setOutputCol("result").
           setVectorSize(6).
           setMinCount(0)

  /*
  * Train the model with the dataset
  * */
  val model = word2Vec.fit(documentDF)

  /*println(model.getClass().getName)
  org.apache.spark.ml.feature.Word2VecModel*/

  /*
  * transform the data set to word vector
  * */
  val result = model.transform(documentDF)

  /*println(result.getClass().getName)
  org.apache.spark.sql.Dataset*/

  result.show()

  /*
  * +--------------------+--------------------+
|                text|              result|
+--------------------+--------------------+
|[Hi, I, heard, ab...|[0.01390241272747...|
|[I, wish, Java, c...|[0.01954013934092...|
|[Logistic, regres...|[-2.5894027203321...|
+--------------------+--------------------+
  * */
  result.select("result").take(3).foreach(println)

  /*
  * [[0.013902412727475166,0.00704740546643734,0.00576745766447857,-0.03196578547358513,0.0022785402019508184,0.030404809676110745]]
[[0.01954013934092862,0.010227076576224394,0.008941462795649256,0.01654639121677194,-0.03726007044315338,-0.00852930758680616]]
[[-2.5894027203321457E-4,0.025160790234804154,-0.001287880726158619,-0.024124881252646446,0.0072902611456811435,-0.008568133413791658]]
  *
  * */
}
}
