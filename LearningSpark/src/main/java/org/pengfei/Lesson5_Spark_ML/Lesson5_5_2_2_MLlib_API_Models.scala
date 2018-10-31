package org.pengfei.Lesson5_Spark_ML

import java.util

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.ml.clustering.GaussianMixture
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.clustering.{GaussianMixtureModel, KMeans, KMeansModel, PowerIterationClustering}
import org.apache.spark.mllib.evaluation.{BinaryClassificationMetrics, MulticlassMetrics, RegressionMetrics}
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionModel, LinearRegressionWithSGD}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.mllib.tree.{DecisionTree, GradientBoostedTrees, RandomForest}


object Lesson5_5_2_2_MLlib_API_Models {

  /***********************************************************************************************************
    * ************************************5.5.2.2 MLlib API models *********************************************
    * *****************************************************************************************************/

  def main(args:Array[String])={
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val spark = SparkSession.builder().master("local[2]").appName("Lesson5_5_2_2_MLlib_API_Models").getOrCreate()

    /*
    * In this lesson, we describes some MLlib’s abstractions for representing machine learning algorithms and models.
    * As we said before, the MLlib is always growing. So the models which we present here is the current state of MLlib
    * of year 2017.
    *
    * A Algorithm describes how model works to solve the ml problem, A model is the ensemble of all parameters(weights,
    * intercepts, etc) which describes the result of a algo applied on a dataset. The model can be used to predict
    * result of unkown data.
    *
    * A model in MLlib is represented by a class. MLlib provides different classes for representing models
    * trained with different machine learning algorithms.
    *
    * Similarly, a machine learning algorithm is represented by a class. MLlib also generally provides a companion
    * singleton object with the same name for each machine learning algorithm class. It is more convenient to
    * train a model using a singleton object representing a machine learning algorithm.
    *
    * Training and using a model generally involves two key methods: train and predict. The train method
    * is provided by the singleton objects representing machine learning algorithms. It trains a model with a given
    * dataset and returns an instance of an algorithm-specific model class. The predict method is provided by the
    * classes representing models. It returns a label for a given set of features.
    *
    * Spark comes prepackaged with sample datasets that can be used to experiment with the MLlib API. For
    * simplicity, in this lesson, the examples use those sample datasets.
    *
    * Some of the data files are in the LIBSVM format. Each line stores an observation. The first column is a
    * label. It is followed by the features, which are represented as offset:value, where offset is the index into the
    * feature vector, and value is the value of a feature.
    *
    * The MLlib library provides helper functions that create RDD[LabeledPoint] from files containing
    * labeled data in the LIBSVM format. These methods are provided by the MLUtils object, which is available in
    * the org.apache.spark.mllib.util package.
    * */

    /***********************************5.5.2.2.1 MLlib API Regression models **********************************/
    //LinearRegressionWithSGDExample(spark)
   // RandomForestExample(spark)

    /***********************************5.5.2.2.2 MLlib API Classification models **********************************/
    SVMWithSGDExample(spark)
   // DecisionTreeExample(spark)
    /***********************************5.5.2.2.3 MLlib API Clustering models **********************************/
   // KMeansExample(spark)
    //PowerIterationExample(spark)

    /***********************************5.5.2.2.4 MLlib API Recommendation models **********************************/
   // ALSExample(spark)
  }

  /***********************************************************************************************************
    * ************************************5.5.2.2.1 MLlib API Regression models ******************************
    * *****************************************************************************************************/

  /*
  * The list of MLlib classes representing different regression algorithms includes
  * - LinearRegressionWithSGD (deprecated since spark 2.0.0)
  * - RidgeRegressionWithSGD
  * - LassoWithSGD
  * - ElasticNetRegression
  * - IsotonicRegression
  * - DecisionTree
  * - GradientBoostedTrees
  * - RandomForest.
  * MLlib also provides companion singleton objects with the same names. These classes and objects provide methods
  * for training regression models.*/

  /*
  * All examples in this Lesson use singleton objects for training a model. There are two most important methods in
  * these singleton objects of ML algo:
  * - train
  * - predict
  *
  * The train method of a regression algorithm object trains or fits a linear regression model with a dataset
  * provided to it as input. It takes an RDD of LabeledPoints as an argument and returns an algorithm-specific
  * regression model.
  *
  * For example, the train method in the LinearRegressionWithSGD object returns an
  * instance of the LinearRegressionModel class. Similarly, the train method in the DecisionTree object
  * returns an instance of the DecisionTreeModel class.
  *
  * The train method also takes a few additional algorithm-specific hyperparameters as arguments. For
  * example, the train method in the RidgeRegressionWithSGD object takes as arguments the number of
  * iterations of gradient descent to run, step size for each gradient descent iteration, regularization parameter,
  * fraction of data to use in each iteration, and initial set of weights. Similarly, the train method in the
  * GradientBoostedTrees object takes a boosting strategy as an argument.*/

  def LinearRegressionWithSGDExample(spark:SparkSession):Unit={

    /******************************LinearRegressionWithSGD*******************************************/
    /* linear regression model with no regularization using Stochastic Gradient Descent. This solves the least
     * squares regression formulation f(weights) = 1/n ||A weights-y||^2 (which is the mean squared error).
     * */

    /* Step 1. prepare data
    * The org.apache.spark.mllib.util.MLUtils lib provdie method which can read libsvm format and return a LabeledPoint
    * RDD and return a Labeled point RDD. The we split the rdd into train and test with a ration 0.8 to 0.2*/
    val filePath="/DATA/data_set/spark/basics/Lesson5_Spark_ML/sample_regression_data.txt"
    val splited=splitData(spark,filePath)
    val train=splited._1
    val test=splited._2

    /* Step 2. Define hyper parameters for the model (LineraRegressionWithSGD). In our case, it's the number
    * of iterations. We choose 100 for speed, it's not optimal for accuracy.
    * */
    val numIterations = 100

    // since spark 2.0, the below is deprecated. Use ml.regression.LinearRegression or LBFGS
    val lrModel=LinearRegressionWithSGD.train(train,numIterations)
// check the model parameters after training, In a linear model Y=a1X1+a2X2+...+C
    // Y is the label
    // X1, X2, ... are the features, a1,a2,... are the weights of these labels
    // C is the intercept (constant) which is the expected mean value of Y when all features=0
    val intercept=lrModel.intercept
    val weights=lrModel.weights
println(s"The intercept has value: ${intercept}, weights has value: ${weights}")

    /* Step 3. predict with the test data*/
    val predictResult=lrModel.predict(test.map{ob=>ob.features})
    println(s"test result ${predictResult.toString()}")

    /* Step 4. validation/Evaluation of the model*/
    //Create a rdd paire of actual and predict value of the observation
    val actualAndPredict=test.map{ob=>(ob.label,lrModel.predict(ob.features))}
    // create an instance of the RegressionMetrics class
    val regressionMetrics=new RegressionMetrics(actualAndPredict)
    // check the various evaluation metrics
    val mse=regressionMetrics.meanSquaredError
    val rmse=regressionMetrics.rootMeanSquaredError
    val mae=regressionMetrics.meanAbsoluteError

    println(s"The mse value is ${mse}, The rmse value is ${rmse}, The mae value is ${mae}")


    /* Step 5. model persistence*/
    /* We can persists a trianed model to disk by using save method. It takes a sparkContext, and path as
     * argument and saves the source models to the given path */
    val modelPath="/DATA/data_set/spark/basics/Lesson5_Spark_ML/models"
    // lrModel.save(spark.sparkContext,modelPath)
    /* The load method is defined in the companion model objects. It generates an instance of model from a previous
    * saved model. It takes a sparkContext and the path of the saved model as arguments and returns an instance of
    * a model class*/
    val loadedModel=LinearRegressionModel.load(spark.sparkContext,modelPath)

    /**********************************Export Model to Predictive Model Markup language (PMML)*****************/
    /* PMML is an XML-based format for describing and serializing models generated by machine learning algorithms. It
     * enables different applications to share models. With PMML, you can train a model in one application and
     * use it from another application.
     *
     * */
    val pmml=lrModel.toPMML()
    println(s"pmml content is : ${pmml}")
  }

  def RandomForestExample(spark:SparkSession):Unit={
    val filePath="/DATA/data_set/spark/basics/Lesson5_Spark_ML/sample_regression_data.txt"
    val splited=splitData(spark,filePath)
    val train=splited._1
    val test=splited._2

    /*********************************specify hyperparameters*******************************/
    /* categoricalFeaturesInfo input is a Map storing arity of categorical features. An Map entry (n -> k) indicates
     * that feature n is categorical with k categories indexed from 0: {0, 1, ..., k-1} */
    val categoricalFeaturesInfo=Map[Int,Int]()
    // specifies number of trees in the random forest
    val numTrees = 3
    /* FeatureSubsetStrategy specifies number of features to consider for splits at each nod. MLlib supports:
    * - auto : model choese a value based on numTrees, if numTrees==1, FeatureSubsetStrategy is set to all, others
    *          it is set to "onethird".
    * - all
    * - sqrt
    * - log2
    * - onethird*/
    val featureSubsetStrategy = "auto"
    /* impurity specifies the criterion used for information gain calculation. Supported values: "variance" */
    val impurity = "variance"
    /* maxDepth specifies the maximum depth of the tree. Depth 0 means 1 leaf node; depth 1 means 1
    * internal node + 2 leaf nodes. Max leafNumber = 2^depth, Max total node number= 2^0+2^1+...2^depth
    * Suggested value: 4*/
    val maxDepth = 4
    /* maxBins specifies the maximum number of bins to use for splitting features. Suggested value: 100*/
    val maxBins = 32

    /****************************************Tain model*****************************************************/
      /* As Tree models(DecisionTree,RandomForest,gradienBoosted, etc.) can both resolve regression and calssification
      * problem, so we use trainRegressor or trainClassifier instead of train */
  val rfModel=RandomForest.trainRegressor(train,categoricalFeaturesInfo,numTrees,featureSubsetStrategy,impurity,maxDepth,maxDepth,12345)

    /****************************************Predict***********************************************/
/* The predict method of a regression model returns a numerical label for a given set of features. It takes a Vector
* or an RDD[Vector] as argument and returns a value of type Double or an RDD of Double.
*
* Thus, it can be used to either predict a label for an observation or a dataset.
It calculates the mean squared error for the model.*/

    val predictResult=rfModel.predict(test.map{ob=>ob.features})
    /******************************************validation*********************************************/
val actualAndPredictLabel=test.map{
  ob=>(ob.label,rfModel.predict(ob.features))
}
    val regressionMetrics=new RegressionMetrics(actualAndPredictLabel)
    val mse=regressionMetrics.meanSquaredError
    val rmse=regressionMetrics.meanSquaredError
    val mae=regressionMetrics.meanAbsoluteError
    println(s"The mse value is ${mse}, The rmse value is ${rmse}, The mae value is ${mae}")

  }


  /***********************************************************************************************************
    * ************************************5.5.2.2.2 MLlib API Classification algorithms ******************************
    * *****************************************************************************************************/

/*
* The list of MLlib classes representing different classification algorithms includes:
* - LogisticRegressionWithSGD
* - LogisticRegressionWithLBFGS
* - SVMWithSGD
* - NaiveBayes
* - DecisionTree,
* - GradientBoostedTrees
* - RandomForest
* MLlib also provides companion singleton objects with the same names. These classes and objects provide methods
* for training classification models, which are also referred to as classifiers.
*
* In this lesson, all examples uses singleton objects and their method to represent classification algorithms
* for training a model.*/

def SVMWithSGDExample(spark:SparkSession)={
  val filePath="/DATA/data_set/spark/basics/Lesson5_Spark_ML/sample_classification_libsvm_data.txt"
  val splited=splitData(spark,filePath)
  val train=splited._1
  val test=splited._2
  val labels=test.map(ob=>ob.label).distinct().collect().toArray
println(s"distinct label value of test : ${labels.mkString(";")}")

  /********************************specify hyperparameters***********************************/
  val numIterations= 20

  /********************************train model***********************/
val svmModel=SVMWithSGD.train(train,numIterations)
  /**********************************predict******************************/
  val predictResult=svmModel.predict(test.map(ob=>ob.features))
  /************************************validation***************************/
  //Build the paire rdd of predict and actual label
  val predictedAndActualLabels = test.map{
    ob=>(ob.label,svmModel.predict(ob.features))
  }
  // Create an instance of the BinaryClassificationMetric class
  val metrics = new BinaryClassificationMetrics(predictedAndActualLabels)

  // precision by threshold
  val precision=metrics.precisionByThreshold()
  precision.foreach{case(t,p)=>println(s"Threshold : $t, Precision: $p")}

  // recall by threshold
  val recall=metrics.recallByThreshold()
  recall.foreach{case(t,r)=> println(s"Threshold : $t, Recall: $r")}

  //precision-recall curve
  val prc=metrics.pr()
  println(s"precison recall curve : $prc")

  // F-measure, the beta arg=1.0
  val f1Score=metrics.fMeasureByThreshold(1.0)
  f1Score.foreach { case (t, f) =>
    println(s"Threshold: $t, F-score: $f, Beta = 1.0")
  }

  // beta =0.5
  val fScore=metrics.fMeasureByThreshold(0.5)
  fScore.foreach { case (t, f) =>
    println(s"Threshold: $t, F-score: $f, Beta = 0.5")
  }
  // get area under curve metric
  val auRoc=metrics.areaUnderROC()

  val auPrc=metrics.areaUnderPR()

  println(s"auRoc value is ${auRoc}, auPrc value is ${auPrc}")

}

def DecisionTreeExample(spark:SparkSession):Unit={
  val irisPath="/DATA/data_set/spark/basics/Lesson5_Spark_ML/iris_libsvm.txt"
  val splited=splitData(spark,irisPath)
  val train=splited._1
  val test=splited._2
   val labels=test.map(ob=>ob.label).distinct().collect().toArray
  println(s"distinct label value ${labels.mkString(";")}")

  //hyperParameters
  val numClasses=8
  val categoricalFeaturesInfo=Map[Int,Int]()
  // The valide value for impurity are gini, entropy
  val impurity = "gini"
  val maxDepth = 4
  val maxBins =16

  // train model
  val gbtModel=DecisionTree.trainClassifier(train,numClasses,categoricalFeaturesInfo,impurity,maxDepth,maxBins)
  // predict result
  val predictAndActual=test.map{ob=>(gbtModel.predict(ob.features),ob.label)}
  // validation, there are three possible labels in iris dataset. so we need to use MultiClassMetrics
  val metrics=new MulticlassMetrics(predictAndActual)
  //recall by label, we can't use metrics inside foreach or map of an RDD or dataframe, becasue it's not serialisable
  // val arrayLabels=Array(0.0,1.0,2.0)
  labels.foreach(l=>println(s"Recall of label ${l} is ${metrics.recall(l)}"))

  //precision by label
  labels.foreach(l=>println(s"Precision of label ${l} is ${metrics.precision(l)}"))

  //false positive rate by label
  labels.foreach(l=>println(s"FPR of label ${l} is ${metrics.falsePositiveRate(l)}"))

  //F-measure by label
  labels.foreach(l=>println(s"F1 score of label ${l} is ${metrics.fMeasure(l)}"))

  // Confusion matrix
  println(s"Confusion matrix : ${metrics.confusionMatrix}")
  // Overall statistics
  println(s"Summary statistics accuracy= : ${metrics.accuracy}")


}

  /***********************************************************************************************************
    * ************************************5.5.2.2.3 MLlib API Clustering algorithms ******************************
    * *****************************************************************************************************/

/*
* The latest doc https://spark.apache.org/docs/latest/mllib-clustering.html
* The list of MLlib classes representing different clustering algorithms includes
* - KMeans
* - StreamingKMeans
* - Bisecting k-means
* - GaussianMixture
* - Latent Dirichlet allocation (LDA)
* - PowerIterationClustering (PIC)
* MLlib also provides companion singleton objects with the same names.
* The methods provided for training clustering models are briefly described next.
*
* As we explained in previous class, clustering algo works on unknow data, which means no labels. So to train a
* clustering model, we need an RDD of Vector and hyperparameters
*
* The hyperparameter arguments and the type of the returned model depend on the clustering algorithm.
* For example, the hyperparameters accepted by the train method in the KMeans object include the number
* of clusters, maximum number of iterations in each run, number of parallel runs, initialization modes, and
* random seed value for cluster initialization. It returns an instance of the KMeansModel class.*/
/****************************************K-means*****************************************************/
def KMeansExample(spark:SparkSession):Unit={
  // get data from file
  val filePath="/DATA/data_set/spark/basics/Lesson5_Spark_ML/kmeans_data.txt"
  val vectors=getClusterData(spark,filePath)
// println(s"vectors value: ${vectors.first().toString}")
  //define hyperParameters
  val numClusters = 2
  val numIterations = 200

  // train the kmean Model
val kMeanModel=KMeans.train(vectors,numClusters,numIterations)

  // Evaluate clustering by computing within set sum of squared errors
  /* The computeCost method returns the sum of the squared distances of the observations from their nearest
* cluster centers. It can be used to evaluate a KMeans model.*/
val WSSSE=kMeanModel.computeCost(vectors)

  println(s"Within set sum of squared errors = ${WSSSE}")

  // predict can return a cluster index for a given observation. It takes a Vector of features and return a Int
val obs1=Vectors.dense(0.0, 0.0, 0.0)
  val obs2=Vectors.dense(9.0, 8.8, 9.9)
  val obs3=Vectors.dense(5.5, 5.6, 5.0)
val index1=kMeanModel.predict(obs1)
  val index2=kMeanModel.predict(obs2)
  val index3=kMeanModel.predict(obs3)
  // predict can take also RDD[Vector] as argument

  println(s"obs1 in cluster ${index1}, obs2 in cluster ${index2},obs3 in cluster ${index3}")

  /********************************model persistence*******************************************/
  val modelSavePath="/DATA/data_set/spark/basics/Lesson5_Spark_ML/models/kmean-model"
  kMeanModel.save(spark.sparkContext, modelSavePath)

  val loadKMeanModel=KMeansModel.load(spark.sparkContext,modelSavePath)

  val kMeansPMML=kMeanModel.toPMML()
  println(s"kMeans model pmml value: ${kMeansPMML}")
}

 /************************************PowerIteration***********************************************/
  def PowerIterationExample(spark:SparkSession):Unit={
    // get data from file
    val circlesRdd = generateCirclesRdd(spark.sparkContext, 2, 100)

/* The power iteration does not take RDD[Vectors] as argument, It takes RDD[(Long,Long,Double)] */
    val model = new PowerIterationClustering()
      .setK(2)
      .setMaxIterations(20)
      .setInitializationMode("degree")
      .run(circlesRdd)

    val clusters = model.assignments.collect().groupBy(_.cluster).mapValues(_.map(_.id))
    val assignments = clusters.toList.sortBy { case (k, v) => v.length }
    val assignmentsStr = assignments
      .map { case (k, v) =>
        s"$k -> ${v.sorted.mkString("[", ",", "]")}"
      }.mkString(", ")
    val sizesStr = assignments.map {
      _._2.length
    }.sorted.mkString("(", ",", ")")
    println(s"Cluster assignments: $assignmentsStr\ncluster sizes: $sizesStr")
  }



  /***********************************************************************************************************
    * ****************************5.5.2.2.4 MLlib API Recommendation algorithms ******************************
    * *****************************************************************************************************/

  /* MLlib supports collaborative filtering, which learns latent factors describing users and products from
   * a dataset containing only user identifiers, product identifiers, and ratings. Collaborative filtering-based
   * recommendation system can be developed in MLlib using the ALS (alternating least squares) algorithm.
   * MLlib provides a class named ALS, which implements Alternating Least Squares matrix factorization. It also
   * provides a companion singleton object with the same name.
   *
   * The latest doc(spark2.3.1) https://spark.apache.org/docs/latest/mllib-collaborative-filtering.html
   *
   * MLlib supports both ratings and implicit feedbacks. A rating is available when a user explicitly rates a
   * product. For example, users rate movies and shows on Netflix. Similarly, users rate songs on iTunes, Spotify,
   * Pandora, and other music services. However, sometimes an explicit rating is not available, but an implicit
   * preference can be determined from user activities. For example, purchase of a product by a user conveys
   * user’s implicitly feedback for a product. Similarly, a user provides implicit feedback for a product by clicking
   * a like or share button.
   *
   * The methods provided by the ALS object for training a recommendation model are briefly described next.*/


def ALSExample(spark:SparkSession):Unit={
    //Read and parse rating data
    val filePath="/DATA/data_set/spark/basics/Lesson5_Spark_ML/sample_recommendation_data.txt"
    val rating_data=spark.sparkContext.textFile(filePath).map{line=>line.split(",") match {
      case Array(user,item,rate)=>Rating(user.toInt,item.toInt,rate.toDouble)
    }
    }
    //specify hyperparameters
    val rank=10
    // this vaule must be small in local mode, it can case stack overflow exception
    val numIteration=10

    // build the recommendataion model using ALS
    /* The train method of the ALS object trains or fits a MatrixFactorizationModel model with an RDD of Rating.
     * It takes an RDD of Rating and ALS-specific hyperparameters as arguments and returns an instance of the
     * MatrixFactorizationModel class. The hyperparameters for ALS include the number of latent features,
     * number of iterations, regularization factor, level of parallelism, and random seed. The last three are optional.*/
    val model = ALS.train(rating_data,rank,numIteration,0.01)

    //Evaluate the model on rating data
    val usersProducts=rating_data.map{case Rating(user,product,rate)=> (user,product)}
    val predictRate=model.predict(usersProducts).map{
      case Rating(user,product,rate)=>((user,product),rate)
    }
    val actualAndPredsRates = rating_data.map{case Rating(user,product,rate)=>((user,product),rate)}.join(predictRate)

    //calculate the mean square error between the actual and predicte rate value
    val MSE = actualAndPredsRates.map{case ((user,product),(actual,predict)) =>
      val err=(actual-predict)
        err*err
    }.mean()

    println(s"Mean squared Error =${MSE}")

    /*********************************Model persistence*************************************/
    val alsModelPath="/DATA/data_set/spark/basics/Lesson5_Spark_ML/models/ALS"
   // model.save(spark.sparkContext,alsModelPath)

    val loadedModel=MatrixFactorizationModel.load(spark.sparkContext,alsModelPath)
    //ALS does not have toPMML() method

    /***********************************TrainImplicit***********************************/
    /* The trainImplicit method can be used when only implicit user feedback for a product is available. Similar
     * to the train method, it takes an RDD of Rating and ALS-specific hyperparameters as arguments and returns
     * an instance of the MatrixFactorizationModel class.
     * The following code snippet uses implicit feedbacks and the ALS object to train a recommendation model.*/
    //confidence parameter
    val alpha=0.01
    //regularization parameter
    val lambda = 0.01

    val implicitModel=ALS.trainImplicit(rating_data,rank,numIteration,lambda,alpha)

    /*******************************************Recommendation *********************************************/
    /**************predict rating between user and product*************************/

    val uId=1
    val pId=1
    val predictedRate=model.predict(uId,pId)
    println(s"predicted rate value is : ${predictedRate}")
/*The predict method also takes a RDD[(uid,pid)]*/

    /******************recommand product for users *********************************/
/* recommendProducts method takes a uid and productNum as arguments and returns an Array of Rating. Each Rating
 * object includes the given user id, product id and a predicted rating score. The returned Array is sorted by
 * rating score in descending order. A high rating score indicates a strong recommendation.*/
    val numProducts=2
    val recommendedProducts=implicitModel.recommendProducts(uId,numProducts)
    println(s"recommended products for user ${uId} is ${recommendedProducts.toArray.mkString(";")} ")
    /*
    * The recommendProductsForUsers method recommends the specified number of top products for all
    * users. It takes the number of products to recommend as an argument and returns an RDD of users and
    * corresponding top recommended products.*/

    val recommededProductsForAllUsers=implicitModel.recommendProductsForUsers(numProducts)
    println(s"recommended products for all users ${recommededProductsForAllUsers.take(4).toArray.mkString(";")}")

    /******************recommand users for product *********************************/
/* The recommendUsers method recommends the specified number of users for a given product. This method
 * returns a list of users who are most likely to be interested in a given product. It takes a product id and number
 * of users to recommend as arguments and returns an Array of Rating. Each Rating object includes a user id,
* the given product id and a score in the rating field. The array is sorted by rating score in descending order*/
  val usersNum=2
    val recommendedUsers=implicitModel.recommendUsers(pId,usersNum)
    println(s"The recommended users for product ${pId} are ${recommendedUsers.toArray.mkString(";")}")

    /* The recommendUsersForProducts method recommends the specified number of users for all products.
* It takes the number of users to recommend as an argument and returns an RDD of products and
* corresponding top recommended users.*/

val recommendedUsersForAllProducts=implicitModel.recommendUsersForProducts(usersNum)
    println(s"Recommeded Users for all products ${recommendedUsersForAllProducts.take(3).toArray.mkString(";")}")
  }


  /***********************************************************************************************************
    * ****************************5.5.2.2.5 MLlib API Model Evaluation ******************************
    * *****************************************************************************************************/

/*
* As mentioned in each model example, evaluating a machine learning model before it is used with new data is an
* important step. We often use quantitative metrics to evaluate the effectiveness of a model.
 *
 * MLlib comes prepackaged with classes that make it easy to evaluate models. These classes are available in the
 * org.apache.spark.mllib.evaluation package. The list of model evaluation related classes includes:
 * - BinaryClassificationMetrics
 * - MulticlassMetrics
 * - MultilabelMetrics
 * - RankingMetrics
 * - RegressionMetrics
 *
 * You can find doc of spark 2.3.1 https://spark.apache.org/docs/2.3.1/mllib-evaluation-metrics.html*/

/*************************************** Regression metrics ***********************************************/

/* The RegressionMetrics class can be used for evaluating models generated by regression algorithms. It provides
* methods for calculating the following metrics
* - mean squared error
* - root mean squared error
* - mean absolute error
* - R2
* - etc.
*
* You can find the example code in line 145 of section 5.5.2.2.1 MLlib api regression model
* */

/**************************************Binary Classification metrics******************************************/

/* The BinaryClassificationMetrics class can be used for evaluating binary classifiers. It provides
 * methods for calculating receiver operating characteristic (ROC) curve, area under the receiver operating
 * characteristic (AUC) curve, and other metrics.
 *
 * The example demonstrates how to use an instance of the BinaryClassificationMetrics
 * class to evaluate a binary classifier can be found in line 272*/


  /***********************************Multiclass Classification Metrics ***********************************/
  /* The MulticlassMetrics class can be used for evaluating multi-class or multi-nominal classifiers. In a
   * multi-class classification task, a label is not binary. An observation can take one of many possible labels.
   * For example, a model that recognizes the images of animals is a multi-class classifier. An image can have the
   * label cat, dog, lion, elephant, or some other label.
   *
   * For evaluating a multi-nominal classifier, the MulticlassMetrics class provides methods for
   * calculating precision, recall, F-measure, and other metrics.
   *
   * The example code can be found in line 333 .*/

 /*************************************** Multilabel Classification Metrics**********************************/
 /* The MultilabelMetrics class can be used for evaluating multi-label classifiers. In a multi-label classification
  * task, an observation can have more than one label. The difference between a between a multi-label and
  * multi-class dataset is that labels are not mutually exclusive in a multi-label classification task, whereas labels
  * are mutually exclusive in a multi-class classification task. An observation in a multi-class classification task
  * can take on only one of many labels.
  *
  * An example of a multi-label classifier is a model that classifies an animal into different categories such
  * as mammal, reptile, fish, bird, aquatic, terrestrial, or amphibian. An animal can belong to two categories;
  * a whale is a mammal and an aquatic animal.*/

 /***************************************  Recommendation Metrics ***************************************/
 /* The RankingMetrics class can be used for evaluating recommendation models. It provides methods for
  * quantifying the predictive effectiveness of a recommendation model.
  *
  * The metrics supported by the RankingMetrics class include
  * - mean average precision
  * - normalized discounted cumulative gain
  * - precision at k.
  * You can read details about these metrics in the paper titled, "IR evaluation methods for retrieving highly
  * relevant documents" by Kalervo Järvelin and Jaana Kekäläinen.*/



  /* This function read a LibSVM files and return a train data as LabeledPoint type and test data as Vector*/
  def getSplitedData(spark:SparkSession,filePath:String): (RDD[LabeledPoint], RDD[Vector],RDD[Double]) ={

    val all:RDD[LabeledPoint]=MLUtils.loadLibSVMFile(spark.sparkContext,filePath)
    all.cache()
    val splited=all.randomSplit(Array(0.8,0.2),123456)
    val train=splited(0)
    // remove the label of test dat
    val test=splited(1).map{
      case observation:LabeledPoint=>observation.features
    }
    // remove the features
    val testResult=splited(1).map{
      case observation:LabeledPoint=>observation.label
    }

    /*println(s"training data has : ${train.count()} elements")
    println(s"training data sample : ${train.first().toString()}")
    println(s"test data has : ${test.count()} elements")
    println(s"test data sample : ${test.first().toString()}")*/
    return Tuple3(train,test,testResult)
  }

  /* This function read a LibSVM files and return a train data as LabeledPoint type and test data as Vector*/
  def splitData(spark:SparkSession,filePath:String): (RDD[LabeledPoint], RDD[LabeledPoint]) ={

    val all:RDD[LabeledPoint]=MLUtils.loadLibSVMFile(spark.sparkContext,filePath)
    all.cache()
    val splited=all.randomSplit(Array(0.8,0.2),123456)
    val train=splited(0)
    val test=splited(1)

    /*println(s"training data has : ${train.count()} elements")
    println(s"training data sample : ${train.first().toString()}")
    println(s"test data has : ${test.count()} elements")
    println(s"test data sample : ${test.first().toString()}")*/
    return Tuple2(train,test)
  }

  def getClusterData(spark:SparkSession,filePath:String):RDD[Vector]={

    // read the file and split each line into array of doubles
    val arrayOfDoulbes=spark.sparkContext.textFile(filePath).map{
      line=>line.split(" ").map(_.toDouble)
    }
    //convert array of doubles into dense vector
    val vectors=arrayOfDoulbes.map{a=>Vectors.dense(a)}.cache()
    return vectors

  }
/************************************Begin of method for generating data for ********************************************/
//nCircles is the number of cluster, nPoints is the total point number in the dataset
def generateCirclesRdd(sc: SparkContext, nCircles: Int, nPoints: Int): RDD[(Long, Long, Double)] = {
    val points = (1 to nCircles).flatMap { i =>
      generateCircle(i, i * nPoints)
    }.zipWithIndex
    val rdd = sc.parallelize(points)
    val distancesRdd = rdd.cartesian(rdd).flatMap { case (((x0, y0), i0), ((x1, y1), i1)) =>
      if (i0 < i1) {
        Some((i0.toLong, i1.toLong, gaussianSimilarity((x0, y0), (x1, y1))))
      } else {
        None
      }
    }
    distancesRdd
  }

  def generateCircle(radius: Double, n: Int): Seq[(Double, Double)] = {
    Seq.tabulate(n) { i =>
      val theta = 2.0 * math.Pi * i / n
      (radius * math.cos(theta), radius * math.sin(theta))
    }
  }

  /**
    * Gaussian Similarity:  http://en.wikipedia.org/wiki/Radial_basis_function_kernel
    */
  def gaussianSimilarity(p1: (Double, Double), p2: (Double, Double)): Double = {
    val ssquares = (p1._1 - p2._1) * (p1._1 - p2._1) + (p1._2 - p2._2) * (p1._2 - p2._2)
    math.exp(-ssquares / 2.0)
  }


}
