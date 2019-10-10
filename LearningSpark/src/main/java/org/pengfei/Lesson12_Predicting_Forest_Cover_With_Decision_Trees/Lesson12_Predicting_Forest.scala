package org.pengfei.Lesson12_Predicting_Forest_Cover_With_Decision_Trees

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.{DecisionTreeClassifier, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{VectorAssembler, VectorIndexer}
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.classification.RandomForestClassificationModel

import scala.util.Random

object Lesson12_Predicting_Forest {

  /**************************************************************************************************************
    * **********************************12.1 Predicting Forest with decision tree *******************************
    * ***********************************************************************************************************/

  /* The data set used in this Lesson is the well know Covtype data set, you can download it from :
  * https://archive.ics.uci.edu/ml/machine-learning-databases/covtype/  It contains a compressed CSV-format data
  * file, covtype.data.gz, and accompanying info file, covtype.info.
  *
  * This data set records the types of forest-covering parcels of land in Colorado, USA. Each example contains
  * several features describing each parcel of land (e.g. elevation, slope, distance to water, shade, and soil type)
  * along with the known forest type covering the land. The forest cover type is to be predicted from the rest of the
  * features, of which there are 54 in total
  *
  * This data set contains both categorical and numeric features. There are 581,012 examples in the data set, which
  * does not exactly qualify as big data, but it still highlight some issues of scale.*/

  def main(args:Array[String]):Unit={
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val spark=SparkSession.builder().appName("Lesson12_Predicting_Forest_Cover").master("local[2]").getOrCreate()
    import spark.implicits._

    /************************************* 12.2 Preparing the data ***************************************************/
    /* The data file does not have column names in header, it's specified in the covtype.info file
     * Conceptually, each column of a CSV file has a type as well (e.g. number, string) but it's not explicit.
     * We need to define the schema ourself*/
    val sparkConfig = ConfigFactory.load("application.conf").getConfig("spark")
    val  path= sparkConfig.getString("sourceDataPath")
    val filePath=s"${path}/spark_lessons/Lesson12_Predicting_Forest_Cover/covtype.data"
    val dataWithoutHeader = spark.read.option("inferSchema", true).option("header",false).csv(filePath)

   // dataWithoutHeader.show(5)
    val colNum=dataWithoutHeader.columns.length
    val rowNum=dataWithoutHeader.count()
    println(s"covtype data set has ${rowNum} rows, and ${colNum} columns")

    /* We could notice that some columns are numeric, some columns are binary(0,1). The binary columns (a group of them)
     * represent an actual categorical column by using "one-hot (1-of-n) encoding". For example, a categorical feature
     * for weather that can be cloudy, rainy, or clear would become three binary features(cloudy, rainy, clear), for
     * each row, if rainy then cloudy column = 0, clear = 0 and rainy = 1.
     *
     * Be careful when encoding a categorical feature as a single numeric feature. The original categorical values
     * have no ordering, but when encoded as a number, they appear to. Treating the encoded feature as numeric
     * leads to meaningless results because the algorithm is effectively pretending that rainy is somehow greater
     * than, and two times larger than, cloudy. It’s OK as long as the encoding’s numeric value is not used as a
     * number.*/

    /* Based on the covtype.info file, the column names show in order in the following sequence. */
    val colNames=Seq("Elevation","Aspect","Slope",
                     "Horizontal_Distance_To_Hydrology", "Vertical_Distance_To_Hydrology",
                     "Horizontal_Distance_To_Roadways",
                     "Hillshade_9am", "Hillshade_Noon", "Hillshade_3pm",
                     "Horizontal_Distance_To_Fire_Points")
                    // Above is the numeric feature
                    // 5 columns of one-hot encoding categorical feature about wilderness area
                   .union((0 until 4).map(i => s"Wilderness_Area_$i"))
                   // The 41 columns of one-hot encoding categorical feautre about soil type
                   .union((0 until 40).map(i => s"Soil_Type_$i"))
                 // The label column of tree cover type
                     .union(Seq("Cover_Type"))
                 // union can be replaced by ++, but it must be on the same line
   // println(s"colNames: ${colNames.toString()}")

    val temp=dataWithoutHeader.toDF(colNames:_*)
     val rawData=temp.withColumn("Cover_Type", temp("Cover_Type").cast("double"))
    /* show all the possible cover_type(label) value*/
   // rawData.select("Cover_Type").distinct().show()

  //  rawData.show(5)

    val Array(trainData,testData)=rawData.randomSplit(Array(0.9,0.1))
    trainData.cache()
    testData.cache()

/********************************** 12.3.1 Build first decision tree model ****************************************/

    /* In this lesson, we only used spark.ml Api, we don't use spark.mllib Api. It's quite different. Be ware of that.*/
    // x=>x can be replaced by _ , so filter(_ !="Cover_Type") works too
    val inputCols=trainData.columns.filter(x=>x!="Cover_Type")

    //Build a assembler which can transform dataframe to a vector
    val assembler = new VectorAssembler().setInputCols(inputCols).setOutputCol("featureVector")
    val assembledTrainData= assembler.transform(trainData)

    //assembledTrainData.select("featureVector").show(5)
// It shows (54,[0,1,2,3,4,5,...]) 54 is the size of the vector, [....] is the vector of features

    /* VectorAssembler is an example of Transformer within the current Spark ML  “Pipelines” API. It transforms
     * another DataFrame into a DataFrame, and is composable with other transformations into a pipeline. Later in
     * this Lesson, these transformations will be connected into an actual Pipeline. Here, the transformation is just
     * invoked directly, which is sufficient to build a first decision tree classifier model.*/

    val classifer = new DecisionTreeClassifier()
     .setSeed(Random.nextLong())
      .setLabelCol("Cover_Type")
      .setFeaturesCol("featureVector")
      .setPredictionCol("prediction")

    val model=classifer.fit(assembledTrainData)
     /*this shows decision tree structure, which consists of a series of nested decisions about features.*/

    //println(model.toDebugString)

    /* decision trees are able to assess the importance of the input features as part of their building process. They
     can estimate how much each feature contriburtes to making correct predictions.*/

   // model.featureImportances.toArray.zip(inputCols).sorted.reverse.foreach(x=>println(s"feature name : ${x._2} has importance: ${x._1}"))

    /* The higher the importance value is better. Elevations seems to be the most important feature. Most features are
    * estimated to have no importance.
    *
    * The resulting DecisionTreeClassificationModel is itself a transformer because it can transform a data frame
    * containing feature vectors into a data frame also containing predictions. */

    /* We can do predictions on training data and compare its prediction with the known correct cover type*/

    // transform here act as a predication
    val predictions=model.transform(assembledTrainData)
    predictions.select("Cover_Type","prediction","probability").show(truncate = false)

    /* The output also contains a "probability" column that gives the model's estimate of how likely it is that
    * each possible outcome is correct. This shows that in these instances, it’s fairly sure the answer is 3 in
    * several cases and quite sure the answer isn’t 1
    *
    * We know that the cover_type only has 7 possible values(1 until 7), but the probability outcomes contains 8 values
    * That's because of the first element of the probability vector always starts with index 0, so we can ignor it as
    * we don't have cover_type 0.*/

    /* We could notice that, there are many predictions are wrong. So we need to work on the hyperparameters of the
    * decisionTreeClassifier.
    *
    * MulticlassClassificationEvaluator can compute accuracy and other metrics that evaluate the quality of the model's
    * predictions. It's an example of an evaluator in spark ml, which is responsible for assessing the quality of
    * an output dataframe in some way*/

   /* val evaluator=new MulticlassClassificationEvaluator()
      .setLabelCol("Cover_Type")
      .setPredictionCol("prediction")*/

   // val accuracy=evaluator.setMetricName("accuracy").evaluate(predictions)
   // val f1=evaluator.setMetricName("f1").evaluate(predictions)
    /*F1 score is specific for binary classification. It consider both the precison(positive predictive value) P and
    * the recall(aka. sensitivity) R.
    * P is the number of correct positive results divided by the number of all positive results.
    * R is the number of correct positive results divided by all relevant samples
    *
    * In a prediction, you have four type result:
    * - 1. true positives
    * - 2. false negatives
    * - 3. false positives
    * - 4. true negatives
    * - 5. relevant elements = (true positives)+(false negatives)
    * - 6. precision = (true positives)/ (all positives in prediction)
    * - 7. recall = (true positives)/ (all relevant elements)
    *
    * For example, we have a binary classification problem of dogs and not dogs, we have 20 rows in this dataset, and 12
    * of them are dogs, but our classifier only identified 8 dogs. 5 of the 8 classification is correct, so
    * 5 dogs prediction is correct, 3 dogs prediction is wrong
    *
    * - all positives = 12 (12 dogs in total in this dataset)
    * --- positives in predictions = 8
    * ----- true positives = 5 (5 correct dog prediction)
    * ----- false positives = 3 (3 wrong dog prediction)
    * - all negatives = 8
    * --- negatives in predictions = 12
    * ----- false negatives = 7 (7 not-dogs in prediction is actually dogs)
    * ----- true negatives = 5 (5 correct not-dogs prediction)
    *
    * - precision = 5/8
    * - recall= 5 / (5+7)
    *
    * */


   // println(s"accuracy is ${accuracy}, f1 is ${f1}")

    /* Confusion matrix is another good way to evaluate models. A confusion matrix is a table with a row and a column
     * for every possible value of the target. In our example, it will be a 7*7 matrix, where each row corresponds to
     * an actual correct value, and each column to a predicted value, in order. The entry at row i and column j counts
     * the number of times an example with true category i was predicted as category j. So the correct predictions are
     * the counts alone the diagonal and the predictions are everything else.
     *
     * In spark, only spark-mllib api provide this implementation, which is based on the rdds. So we need to transform
     * the prediciton:dataframe into RDDs. */
         /* as[] convert dataframe to a dataset[(Double,Double)] */
        // val predictionRDD=predictions.select("prediction","Cover_Type").as[(Double,Double)].rdd
        // val multiClassMetrics=new MulticlassMetrics(predictionRDD)
        // val confusionMatrix=multiClassMetrics.confusionMatrix
        // println(s"confusionMatrix : \n${confusionMatrix}")
   /* The vaules in confusionMatrix will be slightly different each time, because the process of building a decision
   * tree includes some random choices that can lead to different classification.
   *
   * Counts in the diagonal are high, which is good. However, there are certainly a number of misclassifications. For
   * example, category 5, 6 are never predicted at all.*/

    /* We can also build our confusionMatrix ourselves with the help of pivot method of dataframe*/

   // val confusionMatrixDf=predictions.groupBy("Cover_Type").pivot("prediction",(1 to 7)).count.na.fill(0.0).orderBy("Cover_Type")
   // confusionMatrixDf.show()

    /********************************** 12.4 Decision Tree Hyperparameters ****************************************/
/* With default hyperparameters, our decision tree model has about 70% accuracy. A decision Tree model has the
* following important hyperparameters:
* - maximum depth : It simply limits the number of levels in the decision tree. It is the maximum number of chained
*                   decisions that the classifier will make to classify an example. It is useful to limit this to
*                   avoid overfitting the training data.
*
* - maximum bins : It simply limits the total number of sets of values to put in decision rules. For example, for
*                  numeric feature, we have decision rules like weight >= 100, for categorical feature, we have rules
*                  like eye-color in (bleu,green) . The set of values such as 100 and (blue, green) are called bins.
*                  A larger number of bins requires more processing time but might lead to finding a more optimal
*                  decision rule.
*
* - impurity measure : A good decision rule can divide the training data's target values into relatively homogeneous,
*                      or "pure" subsets. Picking a best rule means minimizing the impurity of the two subsets it
*                      induces. There are two common used measures of impurity: Gini impurity and entropy.
*
* - minimum information gain : It's a hyperparameter that imposes a minimum information gain, or decrease in impurity,
*                              for candidate decision rules. Rules that do not improve the subsets impurity enough
*                              are rejected. Like a lower maximum depth, this can help the model resist overfitting
*                              because decisions that barely help divide the training input may in fact not helpfully
*                              divide future data at all.
*           */

    /********************************************* 12.5 Tuning Decision Trees *************************************/

    /* By modifying the hyperparameters which we mentioned above, we can tune the accuracy of the decision tree model
     * To make the test of the best combination of hyperparameters easier, we will create a pipeline which chain the
     * VectorAssembler and DecisionTreeClassifier (two transformers) together. */

    // DecisionTreePipeLine(trainData)

    /************************************************** 12.5.1 Overfitting issue ************************************/

    /* As we discussed previously, it's possible to build a decision tree so deep and elaborate that if fits the
    * given training example very well or perfectly but fails to generalize to other examples. Because it has fit
    * the idiosyncrasies and noise of the training data too closely.
    *
    * A decision tree has overfit, it will exhibit high accuracy when run on the same training data that it fit the
    * model to, but low accuracy on other examples. Here the final model's accuracy was about 91% on new data, but it
     * can easily give an 95% accuracy on the train data.
     *
     * The difference is not large, but suggests that the decision tree has overfit the training data to some extent.
     * A lower maximum depth might be a better choice.*/

    /***************************************** 12.6 Categorical Features Revisited *******************************/

    /* Another way to improve your model is to do feature engieuring. In this section, we will reviste the categorical
    * Features.
    *
    * So far, we have treated all input features as if they're numeric. The label column is encoded as numeric, but has
     * actually been correctly treated as a categorical value. All the categorical value has been encoded with one-hot
     * code.
     *
     * For the binary categorical feature, the one-hot encoding will turn into several binary 0/1 values. Treating
     * these feature as numeric is fine, because any decision rule on these features will choose thresholds between 0
     * and 1, and all are equivalent since all values are 0 or 1.
     *
     * But for complex features, we need to do more to tune the model. For example, we have nine different soil types,
     * and we use many features to describe one soil type. If soil type were encoded as a single categorical feature
     * with 40 different soil value, the decision tree model could have rules like "if the soil type is one of the nine
     * types" directly. However, when encoded as 40 features, the tree would have to learn a sequence of nine decisions
     * on soil type to do the same, this expressiveness may lead to better decisions and more efficient trees.
     *
     * What about undoing the one-hot encoding? This would replace, for example, the four columns encoding wilderness
     * type with one column that encodes the wilderness type as a number between 0 and 3. */
    //trainData.show(1)
    //undo the one-hot encoding
    val unOneHotEncode=unencodeOneHot(trainData)
    // unOneHotEncode.show(1)
    /* Use the pipepline api to build decision tree model training pipeline and trainValidationSplit api to build
     * a evaluation matrix for different hyperparameters */
   // DTreePipelineWithCategoricalFeature(unOneHotEncode)

    /*We could see the accuracy increase 2%, so it's better*/

    /****************************************** 12.6 Random Forest *********************************************/
    RandomForestModelWithCategorical(unOneHotEncode)

    /* Random decision forests are appealing in the context of big data because trees are supposed to be built
    * independently, and big data technologies like Spark and Map‐Reduce inherently need data-parallel problems,
    * where parts of the overall solution can be computed independently on parts of the data. The fact that
    * trees can, and should, train on only a subset of features or input data makes it trivial to parallelize
    * building the trees.*/
  }


def DecisionTreePipeLine(trainData:DataFrame):Unit={
  import trainData.sparkSession.implicits._
  val inputCols=trainData.columns.filter(_ !="Cover_Type")
  // group all feautres column to a vector
  val assembler = new VectorAssembler().setInputCols(inputCols).setOutputCol("featureVector")

  // define the classifier with label, feature, and prediction column.
  val dTreeClassifier = new DecisionTreeClassifier()
    .setSeed(Random.nextLong())
    .setLabelCol("Cover_Type")
    .setFeaturesCol("featureVector")
    .setPredictionCol("prediction")

  // define a pipeline which include the two stages. You can add as many stages as you want into the pipeline
  val pipeline = new Pipeline().setStages(Array(assembler,dTreeClassifier))

  /* Spark ML api has built-in support ParamGridBuilder to test different combinations of hyperparameters for the
  * classifier. In the following example, we have chosen four feature columns, For each feature, we have specified
  * two possible values, which means we will compare 16 models with different hyperparameters.*/
val dTreeParamGrid = new ParamGridBuilder()
    .addGrid(dTreeClassifier.impurity,Seq("gini","entropy"))
    .addGrid(dTreeClassifier.maxDepth,Seq(1,20))
    .addGrid(dTreeClassifier.maxBins,Seq(40,300))
    .addGrid(dTreeClassifier.minInfoGain,Seq(0.0,0.05))
    .build()

  /*We also need to define the evaluation metric that will be used to pick the "best" hyperparameters. Here we use
  * MulticalssClassificationEvaluator api to calculate accuracy. Note that accuracy is not a very good metric for eval*/
  val dTreeMultiClassEval=new MulticlassClassificationEvaluator()
    .setLabelCol("Cover_Type")
    .setPredictionCol("prediction")
    .setMetricName("accuracy")


  /* Finally, we will use trainValidationSplit api to bring all the above components together. We could also use a
  * crossValidator to perform full k-fold cross-validation, but it's k times more expensive and doesn't add as much
  * value in the presence of big dat. So We use TrainValidationSplit.
  *
  * We set the trainRation to 0.9, which means the training data is actually further subdivided by
  * TrainValidationSplit into 9:1 subsets. */
  val dTreeValidator=new TrainValidationSplit()
    .setSeed(Random.nextLong())
    .setEstimator(pipeline)
    .setEvaluator(dTreeMultiClassEval)
    .setEstimatorParamMaps(dTreeParamGrid)
    .setTrainRatio(0.9)

  val validatorModel=dTreeValidator.fit(trainData)
  val bestModel=validatorModel.bestModel
  val paramMap=bestModel.asInstanceOf[PipelineModel].stages.last.extractParamMap
  println(s"Best model param map: ${paramMap}")

  /*The output looks like this
  * Best model param map: {
	dtc_268a6a35ce49-cacheNodeIds: false,
	dtc_268a6a35ce49-checkpointInterval: 10,
	dtc_268a6a35ce49-featuresCol: featureVector,
	dtc_268a6a35ce49-impurity: entropy,
	dtc_268a6a35ce49-labelCol: Cover_Type,
	dtc_268a6a35ce49-maxBins: 40,
	dtc_268a6a35ce49-maxDepth: 20,
	dtc_268a6a35ce49-maxMemoryInMB: 256,
	dtc_268a6a35ce49-minInfoGain: 0.0,
	dtc_268a6a35ce49-minInstancesPerNode: 1,
	dtc_268a6a35ce49-predictionCol: prediction,
	dtc_268a6a35ce49-probabilityCol: probability,
	dtc_268a6a35ce49-rawPredictionCol: rawPrediction,
	dtc_268a6a35ce49-seed: 2560933445228698700
*
* We can notice that "entropy" worked best as the impurity measure, max depth of 20 is better than 1. It might be
* surprising that the best model was fit within just 40 bins. Lastly, no minimun information gain was better than a
* small minimum which cloud imply that the model is more prone to underfit than overfit.
* */

  /* We can also check the accuracy for each hyperparamater combination*/
  val paramsAndMetrics = validatorModel.validationMetrics
    .zip(validatorModel.getEstimatorParamMaps).sortBy(-_._1)
  paramsAndMetrics.foreach{
    case(metric,params)=> {println(metric)
                           println(params)
                           println()}
  }


}

  def unencodeOneHot(data:DataFrame):DataFrame={
    //build a array of wilderness_area column names, note that 0 until 4 output (0,1,2,3)
    val wildernessCols = (0 until 4).map(i=>s"Wilderness_Area_$i").toArray
    //use VectorAssembler to assemble all wilderness columns into a Vector
    val wildernessAssembler = new VectorAssembler().setInputCols(wildernessCols).setOutputCol("wilderness")
    // Find the index of value 1.0 in the vector
    val unhotUDF = udf((vec: Vector)=> vec.toArray.indexOf(1.0).toDouble)
    /* transform the vector of wilderness by the index of wilderness_i which has value 1.0, and drop all
    * column of wilderness_i */
    val withWilderness = wildernessAssembler.transform(data)
      .withColumn("wilderness",unhotUDF(col("wilderness")))
      .drop(wildernessCols:_*)

    /*Repeat the same operations for soil type*/
    val soilCols = (0 until 40).map(i => s"Soil_Type_$i").toArray
    val soilAssembler = new VectorAssembler().setInputCols(soilCols).setOutputCol("soil")
    val withSoil=soilAssembler.transform(withWilderness)
      .withColumn("soil",unhotUDF(col("soil")))
      .drop(soilCols:_*)
    return withSoil

    /* After unOneHotCode, we have one column wilderness (categorical contains 4 values), and one column soil
    * (categorical contains 40 values). With our above operation, if in the origin dataframe, a row has
    * Wilderness_Area_2=1.0, and other Wilerness_Area_i=0.0, in the new dataframe, the wilderness column will
    * have value 2 (0,1,2,3 are the possible values) */
  }

  def DTreePipelineWithCategoricalFeature(data:DataFrame):Unit={
    val inputCols=data.columns.filter(col=>col!="Cover_Type")
    val assembler=new VectorAssembler().setInputCols(inputCols).setOutputCol("featureVector")

    val indexer = new VectorIndexer()
      // here we set MaxCategories = 40, because soil has 40 values
      .setMaxCategories(40)
      .setInputCol("featureVector")
      .setOutputCol("indexedVector")

    val dTreeClassifier=new DecisionTreeClassifier()
      .setSeed(Random.nextLong())
      .setLabelCol("Cover_Type")
      .setFeaturesCol("indexedVector")
      .setPredictionCol("prediction")

    val pipeline = new Pipeline().setStages(Array(assembler,indexer,dTreeClassifier))

    val dTreeParamGrid = new ParamGridBuilder()
      .addGrid(dTreeClassifier.impurity,Seq("gini","entropy"))
      .addGrid(dTreeClassifier.maxDepth,Seq(15,20))
      .addGrid(dTreeClassifier.maxBins,Seq(40,50))
      .addGrid(dTreeClassifier.minInfoGain,Seq(0.0,0.05))
      .build()

    val dTreeMultiClassEval=new MulticlassClassificationEvaluator()
      .setLabelCol("Cover_Type")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    val dTreeValidator=new TrainValidationSplit()
      .setSeed(Random.nextLong())
      .setEstimator(pipeline)
      .setEvaluator(dTreeMultiClassEval)
      .setEstimatorParamMaps(dTreeParamGrid)
      .setTrainRatio(0.9)

    val validatorModel=dTreeValidator.fit(data)
    val bestModel=validatorModel.bestModel
    val paramMap=bestModel.asInstanceOf[PipelineModel].stages.last.extractParamMap
    println(s"Best model param map: ${paramMap}")

  }

  def RandomForestModelWithCategorical(data:DataFrame):Unit={

    val inputCols=data.columns.filter(col=>col!="Cover_Type")
    val assembler=new VectorAssembler().setInputCols(inputCols).setOutputCol("featureVector")

    val indexer = new VectorIndexer()
      // here we set MaxCategories = 40, because soil has 40 values
      .setMaxCategories(40)
      .setInputCol("featureVector")
      .setOutputCol("indexedVector")

    val randomForesetClassifier=new RandomForestClassifier()
      .setSeed(Random.nextLong())
      .setLabelCol("Cover_Type")
      .setFeaturesCol("indexedVector")
      .setPredictionCol("prediction")

    val pipeline = new Pipeline().setStages(Array(assembler,indexer,randomForesetClassifier))

    val randomFParamGrid = new ParamGridBuilder()
      .addGrid(randomForesetClassifier.impurity,Seq("gini","entropy"))
        .addGrid(randomForesetClassifier.numTrees,Seq(2,5))
      .addGrid(randomForesetClassifier.maxDepth,Seq(15,20))
      .addGrid(randomForesetClassifier.maxBins,Seq(40,50))
      .addGrid(randomForesetClassifier.minInfoGain,Seq(0.0,0.05))
      .build()

    val randomFMultiClassEval=new MulticlassClassificationEvaluator()
      .setLabelCol("Cover_Type")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    val randomFValidator=new TrainValidationSplit()
      .setSeed(Random.nextLong())
      .setEstimator(pipeline)
      .setEvaluator(randomFMultiClassEval)
      .setEstimatorParamMaps(randomFParamGrid)
      .setTrainRatio(0.9)

    val validatorModel=randomFValidator.fit(data)
    val forestModel=validatorModel.bestModel.asInstanceOf[PipelineModel].stages.last.asInstanceOf[RandomForestClassificationModel]
    /*get best model param map*/
    val paramMap=forestModel.extractParamMap
    println(s"Best model param map: ${paramMap}")

    /* get feature importance of best model */
    forestModel.featureImportances.toArray.zip(inputCols).
      sorted.reverse.foreach(println)

    /* We can also check the accuracy for each hyperparamater combination*/
    val paramsAndMetrics = validatorModel.validationMetrics
      .zip(validatorModel.getEstimatorParamMaps).sortBy(-_._1)
    paramsAndMetrics.foreach{
      case(metric,params)=> {println(metric)
        println(params)
        println()}
    }


  }

}
