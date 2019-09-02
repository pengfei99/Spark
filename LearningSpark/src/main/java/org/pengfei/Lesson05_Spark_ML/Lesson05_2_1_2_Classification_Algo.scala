package org.pengfei.Lesson05_Spark_ML

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Lesson05_2_1_2_Classification_Algo {

  /*****************************************************************************************************
    *************************************5.2.1.2 Classification Algo ***********************************
    * *************************************************************************************************/
  /*
  * Classification algorithms train models that predict categorical values. The dependent or response variable in
  * the training dataset is a categorical variable. In other words, the label is a categorical variable.
  * The model trained by a classification algorithm can be a binary, multi-class, or multi-label classifier.
  *
  * A binary classifier classifies observations into two categories: positive or negative. The predicted label
  * has only two classes.
  *
  * A multi-class classifier predicts a label that can have more than two classes. For example, a multi-class
  * classifier can be used to classify images of animals. The label in this example can be cat, dog, hamster, lion,
  * or some other animal.
  *
  * A multi-label classifier can output more than one label for the same observation. For example, a
  * classifier that categorizes news articles can output more than one label for an article that is related to both
  * sports and business.
  *
  * The commonly used supervised machine learning algorithms for classification tasks include the
  * following algorithms.
  * - Logistic Regression
  * - Support Vector Machine(SVM)
  * - Naive Bayes
  * - Decision Trees
  * - Random Forest
  * - Gradient-Boosted Trees
  * - Neural Network*/

  def main(args:Array[String]):Unit={
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val spark = SparkSession.builder().master("local[2]").appName("Lesson5_2_1_1_Regression_Algo").getOrCreate()

    /*************************************5.2.1.2.1 Logistic Regression***********************************/

  }

  /*****************************************************************************************************
    *************************************5.2.1.2.1 Logistic Regression ***********************************
    * *************************************************************************************************/
  def LogisticRegressionExample(spark:SparkSession):Unit={
    /* The logistic regression algorithm trains a linear model that can be used for classification tasks. Specifically,
    * the generated model can be used for predicting the probability of occurrence of an event.
    *
    * Logistic regression uses a logistic or sigmoid function to model the probabilities for the possible labels
    * of an unlabeled observation. With any input, the sigmoid function will always return a value between 0 and 1.
    * */
  }

  /*****************************************************************************************************
    *************************************5.2.1.2.2 Support Vector Machine ********************************
    * *************************************************************************************************/
  def SupportVectorMachineExample(spark:SparkSession):Unit={
    /* The Support Vector Machine (SVM) algorithm trains an optimal classifier. Conceptually, it learns from a
     * training dataset an optimal hyperplane (see Figure 8-5, page 163) for classifying a dataset. It finds the
     * best hyperplane that separates training observations of one class from those of the other class. The support
     * vectors are the feature vectors that are closest to the separating hyperplane.
     *
     * The best hyperplane is the one with the largest margin between two classes of observations. Margin in
     * this context is the width of a slab that cleanly separates the observations in the training set. In other words,
     * the margin between the separating hyperplane and the nearest feature vectors from both classes is maximal.
     * The diagram in Figure 8-5 illustrates this point.
     *
     * SVM can be used as a kernel-based method. A kernel-based method implicitly maps feature vectors
     * into a higher-dimensional space where it is easier to find an optimal hyperplane for classifying observations
     * (see Figure 8-6). For example, it may be difficult to find a hyperplane that separates positive and negative
     * examples in a two-dimensional space. However, if the same data is mapped to a three or higher dimensional
     * space, it may be easier to find a hyperplane that cleanly separates the positive and negative observations.
     * The Figure 8-6 illustrates this approach.
     *
     * A kernel based method uses a kernel function, which is a similarity function. The kernel function takes
     * two observations as input and outputs their similarity.
     *
     * SVM is a powerful algorithm, but also more compute-intensive than some of the less sophisticated
     * classification algorithms. One of the advantages of SVM is that it works well on datasets that are not linearly
     * separable.*/
  }

  /*****************************************************************************************************
    *************************************5.2.1.2.3 Naive Bayes ***************************************
    * *************************************************************************************************/

  /* The Naïve Bayes algorithm uses Bayes theorem to train a classifier. The model trained by the Naïve Bayes
   * algorithm is a probabilistic classifier. For a given observation, it calculates a probability distribution over a
   * set of classes.
   *
   * Bayes theorem describes the conditional or posterior probability of an event. The mathematical
   * equation for Bayes theorem is shown next. P(A|B)=(P(B|A).P(A))/P(B)
   *
   * In the preceding equation, A and B are events. P(A|B)is the posterior or conditional probability of A
   * knowing that B has occurred. P(B|A) is the posterior probability of B given that A has occurred. P(A) and P(B)
   * are the prior probabilities of A and B respectively.
   *
   * The Naive Bayes algorithm assumes that all the features or predictor variables are independent. That
   * is the reason it is called naïve. In theory, the Naive Bayes algorithm should be used only if the predictor
   * variables are statistically independent; however, in practice, it works even when the independence
   * assumption is not valid.
   *
   * Naïve Bayes is particularly suited for high dimensional datasets. Although it is a simple algorithm, it
   * often outperforms more sophisticated classification algorithms.*/
  def NaiveBayesExample(spark:SparkSession):Unit={

  }

  /*****************************************************************************************************
    *************************************5.2.1.2.4 Trees ***************************************
    * *************************************************************************************************/

  /* Decision trees, random forest, gradient boosted can also be used for classification problem.
  *
  * For regression tasks, each terminal node stores a numeric value; whereas for classification tasks, each
  * terminal node stores a class label. Multiple leaves may have the same class label. To predict a label for an
  * observation, a decision tree model starts at the root node of a decision tree and tests the features against the
  * internal nodes until it arrives at a leaf node. The value at the leaf node is the predicted label.
  * */

  /*****************************************************************************************************
    *************************************5.2.1.2.5 NeuralNetwork ***************************************
    * *************************************************************************************************/
  def NeuralNetworkExample(spark:SparkSession):Unit={
/*
* Neural Network algorithms are inspired by biological neural networks. They try to mimic the brain. A commonly
* used neural network algorithm for classification tasks is the feedforward neural network. A classifier trained by
* the feedforward neural network algorithm is also known as a multi-layer perceptron classifier.
*
* A multi-layer perceptron classifier consists of interconnected nodes (Figure 8-7,Page 166). A node is also
* referred to as a unit. The network of interconnected nodes is divided into multiple layers.
*
* The first layer consists of the inputs to the classifier. It represents the features of an observation. Thus,
* the number of the nodes in the first layer is same as the number of input features.
*
* The input layer is followed by one or more hidden layers. A neural network with two or more hidden
* layer is known as a deep neural network. Deep learning algorithms, which have recently become popular
* again, train models with multiple hidden layers.
*
* A hidden layer can consist of any number of nodes or units. Generally, the predictive performance
* improves with more number of nodes in a hidden layer. Each node in a hidden layer accepts inputs from the
* all the nodes in the previous layer and produces an output value using an activation function.
*
* The activation function generally used is the logistic (sigmoid) function. Thus, a single-layer
* feedforward neural network with no hidden layers is identical to a logistic regression model.
*
* The last layer, also known as output units, represents label classes. The number of nodes in the output
* layer depends on the number of label classes. A binary classifier will have one node in the output layer. A
* k-class classifier will have k output nodes.
*
* In a feedforward neural network, input data flows only forward from the input layer to the output layer
* through the hidden layers. There are no cycles.
*
* The Figure 8-7 shows a feedforward neural network. The feedforward neural network algorithm uses a technique
* known as backpropagation to train a model. During the training phase, prediction errors are fed back to
* the network. The algorithm uses this information for adjusting the weights of the edges connecting the nodes
* to minimize prediction errors. This process is repeated until the prediction errors converge to value less
* than a predefined threshold.
*
* Generally, a neural network with one layer is sufficient in most cases. If more than one hidden layers are
* used, it is recommended to have the same number of nodes in each hidden layer.
* Neural networks are better suited for classifying data that is not linearly separable. An example of a
* classification tasks involving non-linear data is shown in Figure 8-8(page 167).
*
* Neural networks have a few disadvantages. They are difficult to interpret. It is hard to explain what the
* nodes in the hidden layers represent. In addition, neural network algorithms are more compute intensive
* than simpler classification algorithms such as logistic regression.*/
  }
}
