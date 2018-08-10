package org.pengfei.spark.ml.classification

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.{Row, SparkSession}
object LogisticRegressionClassification {

  def main(args: Array[String]): Unit ={
    val spark = SparkSession.builder().
      master("local").
      appName("LogisticRegressionClassification").
      getOrCreate()

    val training_data = spark.createDataFrame(Seq(
      (0L,"a b c d e spark", 1.0),
      (1L,"a b c", 0.0),
      (2L,"spark h h d g ", 1.0),
      (3L,"hah ho ", 0.0),
      (4L,"toto spark", 1.0),
      (5L,"a b c", 0.0),
      (6L,"spark me to", 1.0),
      (7L,"lol no way", 0.0),
      (8L,"z spark", 1.0),
      (9L,"x y z", 0.0),
      (10L,"spark lol", 1.0),
      (11L,"I love u", 0.0)

    )).toDF("id","text","label")


    val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")

    val hashingTF = new HashingTF().setNumFeatures(1000).setInputCol(tokenizer.getOutputCol).setOutputCol("features")

    val lr = new LogisticRegression().setMaxIter(10).setRegParam(0.01)

    val pipeline = new Pipeline().setStages(Array(tokenizer, hashingTF, lr))

    val model = pipeline.fit(training_data)

    val test_data = spark.createDataFrame(Seq(
      (0L, "spark i j k"),
      (1L, "l m n"),
      (2L, "spark a"),
      (3L, "apache hadoop")
    )).toDF("id","text")

    model.transform(test_data).select("id","text", "probability", "prediction").collect().foreach{case Row(id: Long, text: String, prob: Vector, prediction: Double) => println(s"($id, $text) --> prob=$prob, prediction=$prediction")}

  }
  }
