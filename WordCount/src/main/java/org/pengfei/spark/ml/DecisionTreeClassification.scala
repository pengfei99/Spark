package org.pengfei.spark.ml

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.ml.evaluation.{MulticlassClassificationEvaluator, RegressionEvaluator}
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler, VectorIndexer}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.regression.DecisionTreeRegressor
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

object DecisionTreeClassification {


  def main(args: Array[String]): Unit ={

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = SparkSession.builder().
      master("local").
      appName("DecisionTreeClassification").
      getOrCreate()

   // val data = spark.read.text("file:////home/pliu/Documents/spark_input/iris.txt").map(p=>Iris(Vectors.dense(p(0).toDouble,p(1).toDouble,p(2).toDouble, p(3).toDouble),p(4).toString())).toDF()
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
    //As the ml takes vector as features, create a new vector column which groups the four features
    val assembler = new VectorAssembler().setInputCols(Array("sepal_length","sepal_width","petal_length","petal_width")).setOutputCol("rawFeatures")
    val vecDf=assembler.transform(df)
    vecDf.show(5)

    //set label index in the dataframe
    val labelIndexer = new StringIndexer()
      .setInputCol("Label")
      .setOutputCol("indexedLabel")
      .fit(vecDf)

    // set the feature index in the dataframe
    val featureIndexer = new VectorIndexer()
      .setInputCol("rawFeatures")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(4)
      .fit(vecDf)

    // split the data set for training data and test data
    val Array(trainingData, testData)=vecDf.randomSplit(Array(0.8,0.2))

    // set the decision classifier
    val decisionTree = new DecisionTreeClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures")

    // transform the generated prediction to a humain readable predictLabel
    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)

    // build the machine learning pipeline
    val pipeline = new Pipeline().setStages(Array(labelIndexer,featureIndexer,decisionTree,labelConverter))
    // train the model
    val model = pipeline.fit(trainingData)
    // test the model
    val predictions = model.transform(testData)

    // Select example rows to display.
    predictions.select("predictedLabel", "Label", "rawFeatures").show(5)

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    val accuracy = evaluator.evaluate(predictions)
    println("Estimation accuracy :" + accuracy)
    println("Test Error = " + (1.0 - accuracy))

    val treeModel = model.stages(2).asInstanceOf[DecisionTreeClassificationModel]
    println("Learned classification tree model:\n" + treeModel.toDebugString)

    //val total= df.count()
      //println("Total row is :"+total)
    //df.show(10)
    //val setosa= df.filter(df("Label")==="Iris-setosa").count()
    //println("Setosa number is :"+setosa)
    //df.createOrReplaceGlobalTempView("iris")
    //val label = spark.sql("select COUNT(Label) from global_temp.iris where Label = 'Iris-setosa'")
    //label.show()

    //val labelIndexer = new StringIndexer().setInputCol("Label")
  }
}
