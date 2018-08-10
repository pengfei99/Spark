package org.pengfei.spark.ml.classification

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.ml.regression.LinearRegression
import org.pengfei.spark.ml.data.preparation.NullValueEliminator

object LinearRegressionOnCalHousing {
def main(args:Array[String]): Unit ={
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val spark = SparkSession.builder().
    master("local[2]"). //spark://10.70.3.48:7077 remote
    appName("CalHousing").
    getOrCreate()
  //spark.conf.set("")
  import spark.implicits._

  val inputFile = "file:///DATA/data_set/spark/Cali_Housing"

  val schema = StructType(Array(
    StructField("Logitude", DoubleType, true),
    StructField("Latitude", DoubleType, true),
    StructField("HousingMedianAge", DoubleType, true),
    StructField("TotalRooms", DoubleType, true),
    StructField("TotalBedrooms", DoubleType, true),
    StructField("Population", DoubleType, true),
    StructField("Households", DoubleType, true),
    StructField("MedianIncome", DoubleType, true),
    StructField("MedianHouseValue", DoubleType, true)))

  //Get the csv file as dataframe
  val housingDF=spark.read.format("csv").option("delimiter",",").schema(schema).load(inputFile)

  housingDF.show(5)
  val features=Array("Logitude","Latitude","HousingMedianAge","TotalRooms","TotalBedrooms")
  val nullValueEliminator = new NullValueEliminator()
  val cleanDF=nullValueEliminator.removeNullValueOfFeatureColumns(housingDF,features)
  /*for(feature <- features){
    housingDF=housingDF.filter(housingDF(feature).isNotNull)
  }*/


  //nonNullDF.filter("Logitude is null").show
  //Split dataset into trainingData and testData
  val Array(trainingData, testData) = cleanDF.randomSplit(Array(0.8, 0.2))

  //set label column
  val labelColumn = "MedianHouseValue"

  //define assembler to collect the columns into a new column with a single vector - "features"
  val assembler = new VectorAssembler().setInputCols(Array("Logitude", "Latitude","HousingMedianAge","TotalRooms","TotalBedrooms","Population","Households","MedianIncome"))
    .setOutputCol("features")

  //define the linear regression model
  val lr=new LinearRegression().setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8)
    .setLabelCol(labelColumn)
    .setFeaturesCol("features")
      .setPredictionCol("Predicted "+ labelColumn)

  //define pipeline stages
  val stages = Array(assembler,lr)

  //Construct the pipeline
  val pipeline = new Pipeline().setStages(stages)

  //Fit training data to pipeline
  val model = pipeline.fit(trainingData)

  //Get prediction of testing data
  val predictions = model.transform(testData)

  //Evaluate the model error/deviation of the regression using the Root Mean Squared deviation
  val evaluator = new RegressionEvaluator()
    .setLabelCol(labelColumn)
    .setPredictionCol("Predicted " + labelColumn)
    .setMetricName("rmse")

  //compute the error
  val error= evaluator.evaluate(predictions)

  println(error)
}
}

/*
* Data set Features
*
*longitude: continuous.
latitude: continuous.
housingMedianAge: continuous.
totalRooms: continuous.
totalBedrooms: continuous.
population: continuous.
households: continuous.
medianIncome: continuous.
medianHouseValue: continuous.
*
* */