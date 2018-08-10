package org.pengfei.spark.ml.feature.select

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.ChiSqSelector
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

object FeatureSelection_ChiSqSelector {

  def main(args:Array[String])={

    /*
    * Feature selection is a process which finds better feature data, eliminate useless (irrelevant) data
    * There are many feature selection methods available such as mutual information, information gain, and
    * chi square test.
    *
    * Just as machine learning, there are supervised and unsupervised feature selection method
    *
    * Chi square test is a common supervised feature selection method which we will use in this tutorial
    *
    * */

    /*
    * Problem Statement
    *
    * Suppose there are N instances, and two classes: positive and negative.  Given a feature X, we can use Chi Square
    * Test to evaluate its importance to distinguish the class.
    *
    * By calculating the Chi square scores for all the features, we can rank the features by the chi square scores,
    * then choose the top ranked features for model training.
    *
    * This method can be easily applied for text mining, where terms or words or N-grams are features.
    * After applying chi square test, we can select the top ranked terms as the features to build a text mining model.
    * */

    /*
    * Understand Chi Square Test
    *
    * Chi Square Test is used in statistics to test the independence of two events. Given dataset about two events,
    * we can get the observed count O and the expected count E. Chi Square Score measures how much the expected counts
    * E and observed Count O derivate from each other.
    *
    * In feature selection, the two events are occurrence of the feature and occurrence of the class.
    *
    * In other words, we want to test whether the occurrence of a specific feature and the occurrence of a specific
    * class are independent.
    *
    * If the two events are dependent, we can use the occurrence of the feature to predict the occurrence of the class.
    * We aim to select the features, of which the occurrence is highly dependent on the occurrence of the class.
    *
    * When the two events are independent,  the observed count is close to the expected count, thus a small chi square
    * score. So a high value of \chi^2 indicates that the hypothesis of independence is incorrect. In other words,
    * the higher value of the \chi^2 score, the more likelihood the feature is correlated with the class, thus it
    * should be selected for model training.
    * */
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = SparkSession.builder().
      master("local[2]"). //spark://10.70.3.48:7077 remote
      appName("FeatureSelection_ChiSqSelector").
      getOrCreate()
    //spark.conf.set("")
    import spark.implicits._

    val df = spark.createDataFrame(Seq(
      (1, Vectors.dense(0.0, 0.0, 18.0, 1.0), 1),
      (2, Vectors.dense(0.0, 1.0, 12.0, 0.0), 0),
      (3, Vectors.dense(1.0, 0.0, 15.0, 0.1), 0)
    )).toDF("id","features","label")

    println(df.show())

    val selector = new ChiSqSelector()
                       .setNumTopFeatures(1)
                        .setFeaturesCol("features")
                        .setLabelCol("label")
                        .setOutputCol("selected-feature")

    val selector_model = selector.fit(df)
    val result= selector_model.transform(df)

    println(result.show(false))
  }

}
