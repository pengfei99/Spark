package org.pengfei.spark.ml.classification

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.udf
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.optimization.L1Updater
import org.apache.spark.mllib.regression.LabeledPoint


/*In this tutorial, we use the iris dataset*/
object SupportVectorMachineClassification {
def main(args:Array[String]): Unit ={
  /*
  * Init the spark session
  * */
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val spark = SparkSession.builder().
    master("local").
    appName("SVMClassification").
    getOrCreate()

  /*
  * read data from csv file
  * */
  val inputFile="file:///home/pliu/Documents/spark_input/iris.txt"

  val schema = StructType(Array(
    StructField("sepal_length",DoubleType,false),
    StructField("sepal_width",DoubleType,false),
    StructField("petal_length",DoubleType,false),
    StructField("petal_width",DoubleType,false),
    StructField("Label",StringType,false)
  ))
  //Read csv file
  val df = spark.read.format("com.databricks.spark.csv").option("header", "false").schema(schema).load(inputFile)
  //println("full records number: "+ df.count)
  //df.show(5)

  /*
  * As svm classifier can only treat two label classification problems
  * we need to remove one type of iris flower, In this example, I removed
  * versicolor
  *
  * To make it easier for classifer we conver the label from string to int
  * ris-setosa to 1 and Iris-virginica to 0
  * */
  //df.select(df("label")).distinct.show()
  val readyDf=df.filter(df("label")=!="Iris-versicolor")
  //println("processed records number: "+ readyDf.count)
  //readyDf.select(df("label")).distinct.show()
  val convertUDF=udf(convertLabelToNum)
  val cleanDF=readyDf.withColumn("NumLabel",convertUDF(readyDf("label"))).drop("label").withColumnRenamed("NumLabel","label")
  //cleanDF.show(5)

  /*
  * transforme the feature columns into a vector
  * */

  // vecDf.show(5)

  /*
  * transform dataframe  to labeledPoint RDD
  * Before spark 2.0
  * the labeledPoint is in package org.apache.spark.mllib.regression.LabeledPoint
  * The vectors is in org.apache.spark.mllib.linalg.Vectors
  *
  * After spark 2.0
  * the labeledPoint is in package org.apache.spark.ml.feature.LabeledPoint
  * the vectors is in org.apache.spark.ml.linalg.Vectors
  *
  * "sepal_width","petal_length","petal_width"
  * */
  val labeled = cleanDF.rdd.map(row=>LabeledPoint(row.getAs[Double]("label"),Vectors.dense(row.getAs[Double]("sepal_length"),
    row.getAs[Double]("sepal_width"),row.getAs[Double]("petal_length"),row.getAs[Double]("petal_width"))))
  // labeled.foreach(println)
  /*
  * Split the data into training and testing dataset with 70 and 30 ratio
  *
  * */

  val splits=labeled.randomSplit(Array(0.6,0.4),seed=11L)
  val training = splits(0).cache()
  val test=splits(1)

  /*
  * build svmwithsgd model, and it's still in the mllib package,
  * and it's not compatible with spark 2.0 ml package,
  * */

  val numIterations = 1000
  val model = SVMWithSGD.train(training,numIterations)

  // clear the default threshold(门坎), the classifier will show
  // the real classification score
  model.clearThreshold()
  val scoreAndLabels = test.map { point =>
    val score = model.predict(point.features)
    (score, point.label)
  }

 // scoreAndLabels.foreach(println)
  // set the threshold to 0, all the score <0 will be negative predication
  // all the score > 0 will be positive predication
  model.setThreshold(0.0)

  // we build a score metrics to measure the accuracy of the classifier

  val metrics = new BinaryClassificationMetrics(scoreAndLabels)
  val auROC = metrics.areaUnderROC()
  // print the accuracy
  println("Area under ROC = " + auROC)

  // build a new classifer with optimisation
  val svmAlg = new SVMWithSGD()
  svmAlg.optimizer.
          setNumIterations(2000).
           setRegParam(0.1).
           setUpdater(new L1Updater)
  val modelL1 = svmAlg.run(training)
  // Test the new trained classifier with a test data
  // 5.1,3.5,1.4,0.2,Iris-setosa
  val result=modelL1.predict(Vectors.dense(5.1,3.5,1.4,0.2))
  println("prediction result"+result.toString)
}

  // We define a lamba expression function to convert Iris-setosa to 1
  // Iris-virginica to 0
  def convertLabelToNum:(String=>Double)= { label =>
    if(label.equalsIgnoreCase("Iris-virginica"))
       0.0
    else if (label.equalsIgnoreCase("Iris-setosa"))
       1.0
    else  2.0
  }
}

/*
* (-2.627551665051128,0.0)
(-2.145161194882099,0.0)
(-2.3068829871403618,0.0)
(-3.0554378212130096,0.0)
*
*
*
* */