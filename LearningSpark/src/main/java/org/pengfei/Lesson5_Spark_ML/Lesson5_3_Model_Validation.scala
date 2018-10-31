package org.pengfei.Lesson5_Spark_ML

import org.apache.spark.sql.SparkSession

object Lesson5_3_Model_Validation {

  def main(args:Array[String])={

  }

  /***********************************************AUC************************************************************/
def AUCExamples(spark:SparkSession):Unit={

  /*
  * Area under Curve (AUC), also known as Area under ROC, is a metric generally used for evaluating binary
  * classifiers (see Figure 8-9 page 169). It represents the proportion of the time a model correctly predicts
  * the label for a random positive or negative observation. It can be graphically represented by plotting the
  * rate of true positives predicted by a model against its rate of false positives. The best classifier has
  * the largest area under the curve.
  *
  * A model that just randomly guesses the label for an observation will have an AUC score of approximately 0.5.
  * A model with an AUC score of 0.5 is considered worthless. A perfect model has an AUC score of 1.0. It predicts
  * zero false positives and zero false negatives.
  * */

}
  /*************************************************F Mesure ***************************************************/
  def FMesureExample(spark:SparkSession):Unit={
    /* F-measure, also known as F-score or F1 score, is another commonly used metric for evaluating classifiers.
    * Let’s define two other terms—recall and precision—before defining F-measure.
    *
    * Recall is the fraction of the positive examples classified correctly by a model. The formula for calculating
    * recall is shown next.
    *
    * Recall = TP / (TP + FN), where TP = True Positives, and FN = False Negatives
    *
    * Precision is the ratio of true positives to all positives classified by a model. It is calculated using the
    * following formula.
    *
    * Precision = TP / (TP + FP), where TP = True Positives, and FP = False Positives
    *
    * The F-measure of a model is the harmonic mean of the recall and precision of a model. The formula for
    * calculating the F-measure of a model is shown here.
    *
    * F-measure = 2* (precision * recall) / (precision + recall)
    *
    * The F-measure of a model takes a value between 0 and 1. The best model has an F-measure equal to 1,
    * whereas a model with an F-score of 0 is the worst.
    *
    * */
  }

  /*************************************************Root Mean Squared Error (RMSE)*************************************/
def RMSEExample(spark:SparkSession):Unit={
  /* The RMSE metric is generally used for evaluating models generated by regression algorithms. A related
* metric is mean squared error (MSE). An error in the context of a regression algorithm is the difference
* between the actual and predicted numerical label of an observation. As the name implies, MSE is the mean
* of the square of the errors. It is mathematically calculated by squaring the error for each observation and
* computing the mean of the square of the errors. RMSE is mathematically calculated by taking a square root
* of MSE.
*
* RMSE and MSE represent training error. They indicate how well a model fits a training set. They capture
* the discrepancy between the observed labels and the labels predicted by a model.
* A model with a lower MSE or RMSE represents a better fitting model than one with a higher MSE or RMSE.*/
}
}
