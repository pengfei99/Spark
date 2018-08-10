package org.pengfei.spark.ml.classification

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

object SupportVectorMachineRDD {
  def main(args:Array[String]): Unit = {
    /*
    * Init the spark session
    * */
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = SparkSession.builder().
      master("local").
      appName("SVMRDDClassification").
      getOrCreate()

    /*
    * read data from csv file
    * */
    import spark.implicits._
    val inputFile = "file:///home/pliu/Documents/spark_input/iris.txt"

    val data = spark.read.text(inputFile).as[String]

    val parsedData = data.map { line => val parts = line.split(',')
          LabeledPoint(
            if(parts(4)=="Iris-setosa") 0.toDouble
            else if (parts(4) =="Iris-versicolor") 1.toDouble
            else 2.toDouble, Vectors.dense(parts(0).toDouble,parts(1).toDouble,parts
    (2).toDouble,parts(3).toDouble))
          }.rdd
    println(parsedData.getClass.getName)
    //parsedData.show(5)

    val splits = parsedData.filter { point => point.label != 2 }.randomSplit(
      Array(0.6, 0.4), seed = 11L)

    val training = splits(0).cache()
    val test = splits(1)

    val numIterations = 1000
    val model = SVMWithSGD.train(training, numIterations)

    model.clearThreshold()
    val scoreAndLabels = test.map { point =>
            val score = model.predict(point.features)
             (score, point.label)
            }

    scoreAndLabels.foreach(println)
  }
}
