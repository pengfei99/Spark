package org.pengfei.Lesson05_Spark_ML

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Lesson05_2_1_1_Regression_Algo {

  def main(args:Array[String])={
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val spark = SparkSession.builder().master("local[2]").appName("Lesson5_2_1_1_Regression_Algo").getOrCreate()

    /*************************************5.2.1.1.1 LinearRegression***********************************/
  }

  /*****************************************************************************************************
    *************************************5.2.1.1.1 LinearRegression ***********************************
    * *************************************************************************************************/
  def LinearRegressionExample(spark:SparkSession):Unit={
    /* Linear regression algorithms fit a linear model with coefficients using training data. A linear model is a
* linear combination of a set of coefficients and explanatory variables. The algorithm estimates the unknown
* coefficients, also known as model parameters, from training data. The fitted coefficients minimize the sum
* of the squares of the difference between predicted and actual observed labels in the training dataset.
*
* A simple example of a linear model is show as : y = q0+q1X1+q2X2+q3X1X2
* y is the label(dependent variable), X1,X2 are the features(independent variables). The values of y, X1, X2 are
* know for each observation(row in dataset). A linear regression algo estimates the values of q0, q1, q3, q4 using
* the training data. After training, the value of q0, q1, q2, q3 are know, we can use the equation and x values to
* estimate y value.
*
* Note that, the model will never perfect match the real data, you will always have caps between estimate and real value
* */

  }

  /*****************************************************************************************************
    *************************************5.2.1.1.2 Isotonic Regression ***********************************
    * *************************************************************************************************/
  def IsotonicRegressionExample(spark:SparkSession):Unit={
    /* The Isotonic Regression algorithm fits a non-decreasing function to a training dataset. It finds the best
     * least squares fit to a training dataset with the constraint that the trained model must be a non-decreasing
     * function. A least squares function minimizes the sum of the squares of the difference between predicted and
     * actual labels in the training dataset. Unlike linear regression, the Isotonic Regression algorithm does not
     * assume any form for the target function such as linearity.
     *
     * Page 160, figure 8-2 (Big data analytics with spark). This figure show well the difference of linear regression
     * and isotonic regression*/



  }

  /*****************************************************************************************************
    *************************************5.2.1.1.3 Decision Trees ***********************************
    * *************************************************************************************************/

  def DecisionTreesExample(spark:SparkSession):Unit={
    /* The decision tree algorithm infers a set of decision rules from a training dataset. It creates a decision tree
    * that can be used to predict the numeric label for an observation.
    *
    * A tree is a hierarchal collection of nodes and edges. Unlike a graph, there are no loops in a tree.
    * A non-leaf node is called an internal or split node. A leaf node is called a terminal node.
    *
    * In a decision tree, each internal node tests the value of a feature or predictor variable. The observations
    * in the training dataset are segmented into a number of regions using these tests. A leaf node represents a
    * region and stores the average value of all the observations belonging to a region.
    *
    * Given a new unlabeled observation, a decision tree model starts at the root node and evaluates the
    * observation features against the internal nodes. It traverses down a tree until it arrives at a terminal node.
    * The value stored at the matching terminal node is returned as the predicted label. Thus, a decision tree
    * model conceptually implements hierarchal if-else statements. It performs a series of tests on the features to
    * predict a label.
    *
    * Decision trees can be used for both regression and classification tasks. The “Classification Algorithms”
    * section describes how decision trees can be used for classification tasks.
    *
    * The decision tree algorithm has many advantages over other more sophisticated machine learning
    * algorithms. First, models trained by a decision tree are easy to understand and explain. Second, it can easily
    * handle both categorical and numerical features. Third, it requires little data preparation. For example, unlike
    * other algorithms, it does not require feature scaling.
    *
    * The decision tree has the problem of over fitting and under fitting. This problem causes inaccuracy*/


  }


  /*****************************************************************************************************
    *************************************5.2.1.1.4 Ensembles ***********************************
    * *************************************************************************************************/

  /* To slove the problem of decision tree, we combine multiple tree models to generate a more powerful model. There
  * are called ensemble learning algorithms. It uses several base models to improve generalizability and predictive
   * accuracy over a single model. The commonly used algo are:
   * - Random Forests
   * - Cradient-Boosted Trees*/
def RandomForestExample(spark:SparkSession):Unit={
    /* The Random Forest algorithm trains each decision tree in an ensemble independently using a random sample of data.
  * In addition, each decision tree is trained using a subset of the features. The number of trees in an ensemble is of
  * the order of hundreds. Random Forest creates an ensemble model that has a better predictive performance
  * than that of a single decision tree model.
  *
  * For a regression task, a Random Forest model takes an input feature vector and gets a prediction from
  * each decision tree in the ensemble. It averages the numeric labels return by all the trees and returns
  * the average as its prediction.*/
}

  def GradientBoostedTreesExample(spark:SparkSession):Unit={
    /*
    * The Gradient-Boosted Trees (GBTs) algorithm also trains an ensemble of decision trees. However, it
    * sequentially trains each decision tree. It optimizes each new tree using information from previously
    * trained trees. Thus, the model becomes better with each new tree. GBT can take longer to train a model
    * since it trains one tree at a time. In addition, it is prone to overfitting if a large number of trees
    * are used in an ensemble. However, each tree in a GBT ensemble can be shallow, which are faster to train.*/
  }

}
