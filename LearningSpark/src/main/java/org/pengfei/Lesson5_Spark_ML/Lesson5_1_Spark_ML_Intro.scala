package org.pengfei.Lesson5_Spark_ML

object Lesson5_1_Spark_ML_Intro {
def main(args:Array[String])={
  /******************************************************************************************************************
    * *****************************************5.1 ML Introduction *************************************************
    * **************************************************************************************************************/

  /*
  * In simple word, a machine learning algorithm infers patterns and relationships between different
  * variables in a dataset. It then uses that knowledge to generalize beyond the training dataset. In other words,
  * a machine learning algorithm learns to predict from data.*/

  /******************************************************************************************************************
    * *****************************************5.1.1 ML Terminologies *************************************************
    * **************************************************************************************************************/

  /****************************************************Features************************************************
  *
  * A feature represents an attribute or a property of an observation. It is also called a variable. To be more
  * specific, a feature represents an independent variable.
  *
  * In a tabular dataset, a row represents an observation and column represents a feature. For example,
  * consider a tabular dataset containing user profiles, which includes fields such as age, gender, profession,
  * city, and income. Each field in this dataset is a feature in the context of machine learning. Each row
  * containing a user profile is an observation.
  *
  * Features are also collectively referred to as dimensions. Thus, a dataset with high dimensionality has a
  * large number of features.*/

  /****************************************Categorical Features**************************************************
    * A categorical feature or variable is a descriptive feature. It can take on one of a fixed number of discrete
    * values. It represents a qualitative value, which is a name or a label.
    *
    * The values of a categorical feature have no ordering. For example, in the user profile dataset mentioned
    * earlier, gender is a categorical feature. It can take on only one of two values, each of which is a label.
    * In the same dataset, profession is also a categorical variable, but it can take on one of several hundred values.
    * */

  /***************************************Numerical Features******************************************************
    * A numerical feature or variable is a quantitative variable that can take on any numerical value. It describes
    * a measurable quantity as a number. The values in a numerical feature have mathematical ordering. For example,
    * in the user profile dataset mentioned earlier, income is a numerical feature.
    *
    * Numerical features can be further classified into discrete and continuous features. A discrete numerical
    * feature can take on only certain values. For example, the number of bedrooms in a home is a discrete
    * numerical feature. A continuous numerical feature can take on any value within a finite or infinite interval.
    * An example of a continuous numerical feature is temperature.
    * */

  /**************************************************Labels*******************************************************
    * A label is a variable that a machine learning system learns to predict. It is the dependent variable in a
    * dataset. Labels can be a classified into two broad categories: categorical and numerical.
    *
    * A categorical label represents a class or category. For example, for a machine learning application
    * that classifies news articles into different categories such as politics, business, technology, sports, or
    * entertainment, the category of a news article is a categorical label.
    *
    * A numerical label is a numerical dependent variable. For example, for a machine learning application
    * that predicts the price of a house, price is a numerical label.*/

  /*************************************************Models*******************************************************
    * A model is a mathematical construct for capturing patterns within a dataset. It estimates the relationship
    * between the dependent and independent variables in a dataset. It has predictive capability. Given the values
    * of the independent variables, it can calculate or predict the value for the dependent variable. For example,
    * consider an application that forecasts quarterly sales for a company. The independent variables are number
    * of sales people, historical sales, macro-economic conditions, and other factors. Using machine learning, a
    * model can be trained to predict quarterly sales for any given combination of these factors.
    *
    * A model is basically a mathematical function that takes features as input and outputs a value. It can be
    * represented in software in numerous ways. For example, it can be represented by an instance of a class.
    * We will see a few concrete examples later in this Lesson.
    *
    * A model along with a machine learning algorithm forms the heart of a machine learning system. A machine
    * learning algorithm trains a model with data; it fits a model over a dataset, so that the model can predict
    * the label for a new observation.
    *
    * Training a model is a compute intensive task, while using it is not as compute intensive. A model is
    * generally saved to disk, so that it can be used in future without having to go through the compute intensive
    * training step again. A serialized model can also be shared with other applications. For example, a machine
    * learning system may consist of two applications, one that trains a model and another that uses a model.
    * */

  /************************************************Training Data*************************************************
    * The data used by a machine learning algorithm to train a model is called training data or training set. It is
    * historical or known data. For example, a spam filtering algorithm uses a known set of spam and non-spam emails.
    *
    * The training data can be classified into two categories: labeled and unlabeled.
    * */

  /************************************************Labeled*********************************************************
    * A labeled dataset has a label for each observation. One of the columns in the dataset contains the labels.
    * For example, a database of homes sold in the last ten years is a labeled dataset for a machine learning
    * application that predicts the price of a home. The label in this case is home price, which is known for homes
    * sold in the past. Similarly, a spam filtering application is trained with a large dataset of emails, some of
    * which are labeled as spam and others as non-spam.*/

  /***********************************************Unlabeled********************************************************
    * An unlabeled dataset does not have a column that can be used as a label. For example, consider a transaction
    * database for an e-commerce site. It records all the online purchases made through that site. This database
    * does not have a column that indicates whether a transaction was normal or fraudulent. So for fraud detection
    * purposes, this is an unlabeled dataset.
    * */

  /**********************************************Test data*********************************************************
    * The data used for evaluating the predictive performance of a model is called test data or test set. After a
    * model has been trained, its predictive capabilities should be tested on a known dataset before it is used on
    * new data.
    *
    * Test data should be set aside before training a model. It should not be used at all during the training
    * phase; it should not be used for training or optimizing a model. In fact, it should not influence the training
    * phase in any manner; do not even look at it during the training phase. A corollary to this is that a model
    * should not be tested with the training dataset. It will perform very well on the observations from the training
    * set. It should be tested on data that was not used in training it.
    *
    * Generally, a small proportion of a dataset is held out for testing before training a model. The exact
    * percentage depends on a number of factors such as the size of a dataset and the number of independent
    * variables. A general rule of thumb is to use 80% of data for training a model and set aside 20% as test data.
    * */

  /******************************************************************************************************************
    * *****************************************5.1.2 ML Applications *************************************************
    * **************************************************************************************************************/

  /* Machine learning is used for a variety of tasks in different fields. A large number of applications use machine
  * learning, and that number is increasing every day. The machine learning tasks can be broadly grouped into
  * the following categories:
  *  • Classification
  *  • Regression
  *  • Clustering
  *  • Anomaly detection
  *  • Recommendation
  *  • Dimensionality reduction
  *
  *  */


  /*******************************************5.1.2.1 Classification ************************************************/

  /*
  * The goal while solving a classification problem is to predict a class or category for an observation. A class
  * is represented by a label. The labels for the observations in the training dataset are known, and the goal is
  * to train a model that predicts the label for a new unlabeled observation. Mathematically, in a classification
  * task, a model predicts the value of a categorical variable.
  *
  * Classification is a common task in many fields. For example, spam filtering is a classification task. The
  * goal of a spam filtering system is to classify an email as a spam or not. Similarly, tumor diagnosis can be
  * treated as a classification problem. A tumor can be benign or cancerous. The goal in this case is to predict
  * whether a tumor is benign or cancerous. Another example of a classification task is determining credit risk
  * of a borrower. Using information such as an individual’s income, outstanding debt, and net worth, a credit
  * rating is assigned to an individual.
  *
  * Machine learning can be used for both binary and multi-class classification. The previous paragraph
  * described a few examples of binary classification. In binary classification, the observations in a dataset can
  * be grouped into two mutually exclusive classes. Each observation or sample is either a positive or negative
  * example.
  *
  * In multi-class classification, the observations in a dataset can be grouped into more than two classes.
  * For example, handwritten zip-code recognition is a multi-class classification problem with ten classes.
  * In this case, the goal is to detect whether a handwritten character is one of the digits between 0-9. Each
  * digit represents a class. Similarly, image recognition is a multi-class classification task, which has many
  * applications. One of the well-known applications is a self-driving or driver-less car. Another application is
  * Xbox Kinect360, which infers body parts and position using machine learning.
  * */

  /*******************************************5.1.2.2 Regression ************************************************/

  /* The goal while solving a regression problem is to predict a numerical label for an unlabeled observation.
  * The numerical labels are known for the observations in the training dataset and a model is trained to predict
  * the label for a new observation.
  *
  * Examples of regression tasks include home valuation, asset trading, and forecasting. In home valuation,
  * the value of a home is the numerical variable that a model predicts. In asset trading, regression techniques
  * are used to predict the value of an asset such as a stock, bond, or currency. Similarly, sales or inventory
  * forecasting is a regression task.
  *
  * */

  /*******************************************5.1.2.3 Clustering ************************************************/
/*
* In clustering, a dataset is split into a specified number of clusters or segments. Elements in the same cluster
* are more similar to each other than to those in other clusters. The number of clusters depends on the
* application. For example, an insurance company may segment its customers into three clusters: low-risk,
* medium-risk and high-risk. On the other hand, an application may segment users on a social network into
* 10 communities for research purposes.
*
* Some people find clustering confusingly similar to classification. They are different. In a classification
* task, a machine learning algorithm trains a model with a labeled dataset. Clustering is used with unlabeled
* datasets. In addition, although a clustering algorithm splits a dataset into a specified number of clusters, it
* does not assign a label to any cluster. A user has to determine what each cluster represents.
*
* A popular example of clustering is customer segmentation. Organizations use clustering as a data-driven
* technique for creating customer segments, which can be targeted with different marketing programs.
* */
  /*******************************************5.1.2.4 Anomaly Detection *****************************************/
/*
* In anomaly detection, the goal is to find outliers in a dataset. The underlying assumption is that an outlier
* represents an anomalous observation. Anomaly detection algorithms are used with unlabeled data.
*
* Anomaly detection has many applications in different fields. In manufacturing, it is used for
* automatically finding defective products. In data centers, it is used for detecting bad systems. Websites use it
* for fraud detection. Another common use-case is detecting security attacks. Network traffic associated with a
* security attack is unlike normal network traffic. Similarly, hacker activity on a machine will be different from
* a normal user activity.
* */
  /*******************************************5.1.2.5 Recommendation ************************************************/

  /*
* The goal of a recommendation system, also known as recommender system, is to recommend a product to a user.
* It learns from users’ past behavior to determine user preferences. A user rates different products, and
* over time, a recommendation system learns this user’s preferences. In some cases, a user may not explicitly
* rate a product but provide implicit feedback through actions such as purchase, click, view, like, or share.
*
* A recommendation system is one of the well-known examples of machine learning. It is getting
* embedded in more and more applications. Recommendation systems are used to recommend news
* articles, movies, TV shows, songs, books, and other products. For example, Netflix uses recommender
* systems to recommend movies and shows to its subscribers. Similarly, Spotify, Pandora, and Apple use
* recommendation systems to recommend songs to their subscribers.
*
* The two commonly used techniques for building recommendation systems are collaborative filtering
* and content-based recommendation. In collaborative filtering, the properties of a product or user
* preferences are not explicitly programmed. The algorithm assumes that the user preferences and products
* have latent features, which it automatically learns from ratings of different products by different users.
* The input dataset is in a tabular format, where each row contains only a user id, product id, and rating.
* Collaborative filtering learns latent user and product feature just from these three fields. It learns users
* with similar preferences and products with similar properties. The trained model can then be used to
* recommend products to a user. The products recommended to a user are those rated highly by other users
* with similar preferences.
*
* A content-based recommendation system uses explicitly specified product properties to determine
* product similarity and make recommendations. For example, a movie has properties such as genre, lead
* actor, director, and year released. In a content-based system, every movie in a movie database will have
* these properties recorded. For a user who mostly watches comedy movies, a content-based system will
* recommend a movie having genre as comedy.
* */

  /*******************************************5.1.2.6 Dimensionality Reduction***************************************/

  /*
  * Dimensionality reduction is a useful technique for reducing the cost and time it takes to train a machine
  * learning system. Machine learning is a compute intensive task. The computation complexity and cost increases
  * with the number of features or dimensions in a dataset. The goal in dimensionality reduction is to reduce the
  * number of features in a dataset without significantly impacting the predictive performance of a model.
  *
  * A dataset may have so many dimensions that it is prohibitively expensive to use it for machine learning.
  * For example, a dataset may have several thousand features. It may take days or weeks to train a system with
  * this dataset. With dimensionality reduction techniques, it can be used to train a machine learning system in
  * a more reasonable time.
  *
  * The basic idea behind dimensionality reduction is that a dataset may have several features that have low
  * or zero predictive power. A dimensionality reduction algorithm automatically eliminates these features from
  * a dataset. Only the features with most predictive power are used for machine learning. Thus, dimensionality
  * reduction techniques reduce the computational complexity and cost of machine learning.*/


  /******************************************************************************************************************
    * *****************************************5.2 ML Algorithms *************************************************
    * **************************************************************************************************************/

  /* Machine learning algorithms use data to train a model. The process of training a model is also referred to as
  * fitting a model with data. In other words, a machine learning algorithm fits a model on a training dataset.
  * Depending on the type of the training data, machine learning algorithms are broadly grouped into two
  * categories:
  * - supervised machine learning
  * - unsupervised machine learning.*/

  /******************************************************************************************************************
    * *****************************************5.2.1 Supervised ML Algorithms *****************************************
    * **************************************************************************************************************/

  /*
  * A supervised machine learning algorithm trains a model with a labeled dataset. It can be used only with
  * labeled training datasets.
  *
  * Each observation in the training dataset has a set of features and a label. The dependent variable, also
  * known as the response variable, represents the label. The independent variables, also known as explanatory
  * or predictor variables, represent the features. A supervised machine learning algorithm learns from data to
  * estimate or approximate the relationship between a response variable and one or more predictor variables.
  *
  * The labels in a training dataset may be generated manually or sourced from another system. For
  * example, for spam filtering, a large sample of emails are collected and manually labeled as spam or not.
  * On the other hand, for sales forecasting, label will be historical sales, which can be sourced from a sales
  * database.
  *
  * Supervised machine learning algorithms can be broadly grouped into two categories:
  * - Regression
  * - Classification .*/

  /*****************************************5.2.1.1 Regression Algorithms *****************************************/
  /*
  * A regression algorithm trains a model with a dataset that has a numerical label. The trained model can then
  * predict numerical labels for new unlabeled observations.
  *
  * Depending on the number of predictor and response variables, regression tasks can be grouped in three categories:
  * - simple regression : involves one response and one predictor variable
  * - multiple regression : involves one response and multiple predictor variables
  * - multivariate regression : involves several response and several predictor variables.
  *
  * The commonly used supervised machine learning algorithms for regression tasks include:
  * - linear regression
  * - decision trees
  * - ensembles of trees(Random forest).*/

  /* You can get the details of each regression algorithm in separate files Lesson5_2_1_1_Regression_Algo*/

  /*****************************************5.2.1.2 Classification Algorithms *****************************************/

  /* You can get the details of each classification algorithm in separate files Lesson5_2_1_2_Classification_Algo*/

  /******************************************************************************************************************
    * *****************************************5.2.2 Unsupervised ML Algorithms *****************************************
    * **************************************************************************************************************/

  /* An unsupervised machine learning algorithm is used when a dataset is unlabeled. It draws inferences from
 * unlabeled datasets. Generally, the goal is to find hidden structure in unlabeled data. Unsupervised machine
 * learning algorithms are generally used for
 * - clustering
 * - anomaly detection
 * - dimensionality reduction.
 * The list of commonly used unsupervised machine learning algorithms includes k-means, Principal
 * Component Analysis, and Singular Value Decomposition (SVD).*/

  /*****************************************5.2.2.1 Clustering Algorithms *****************************************/
  /* You can get the details of each clustering algorithm in separate files Lesson5_2_2_1_Clustering_Algo*/

  /*****************************************5.2.2.2 Anomaly detection Algorithms *************************************/
  /* You can get the details of each anomaly detection algorithm in separate files Lesson5_2_2_2_Anomaly_Detection_Algo*/

  /*****************************************5.2.2.3 Dimensionality reduction Algorithms *********************************/
  /* You can get the details of each clustering algorithm in separate files Lesson5_2_2_3_Dimensionality_reduction_Algo*/

  /******************************************************************************************************************
    * *****************************************5.2.3 ML Algorithms Hyperparameter ***********************************
    * **************************************************************************************************************/

  /* Many machine learning algorithms also require a few input parameters that determine the training time and
  * predictive effectiveness of a trained model. These parameters, which are not directly learnt, but provided
  * as inputs to a learning algorithm, are known as hyperparameters. A good data scientiest can find the best
  * hyperparameters to improve model accuracy.*/

  /******************************************************************************************************************
    * *****************************************5.3 ML Model Validation ***********************************
    * **************************************************************************************************************/

  /* After a model is trained, it is important to evaluate it on a test dataset. The predictive effectiveness or
  * quality of a model can be evaluated using a few different metrics. Generally, the evaluation metric depends
  * on the machine learning task. Different metrics are used for linear regression, classification, clustering,
  * and recommendation.
  *
  * A simple model evaluation metric is accuracy. It is defined as the percentage of the labels correctly
  * predicted by a model. For example, if a test dataset has 100 observations and a model correctly predicts the
  * labels for 90 observations, its accuracy is 90%.
  *
  * However, accuracy can be a misleading metric. For example, consider a tumors database, where each
  * row has data about either a malignant or a benign tumor. In the context of machine learning, a malignant
  * tumor is considered a positive sample and a benign tumor is considered a negative sample. Suppose we
  * train a model that predicts whether a tumor is malignant (positive) or non-cancerous benign (negative). Is it
  * a good model if it has 90% accuracy?
  *
  * It depends on the test dataset. If the test dataset has 50% positive and 50% negative samples, our model
  * is performing well. However, if the test dataset has only 1% positive and 99% negative samples, our model
  * is worthless. We can generate a better model without using machine learning; a simple model that always
  * classifies a sample as negative will have 99% accuracy. Thus, it has a better accuracy than our trained model,
  * even though it incorrectly classifies all the positive samples.
  *
  * The two commonly used metrics for evaluating a classifier or classification model are:
  * - Area under Curve(AUC)
  * - F-measure.
  * For regression model, we often use
  * - Root Mean Squared Error (RMSE)
  *
  * You can get the details of these validation method in separate files Lesson5_3_Model_Validation*/

  /******************************************************************************************************************
    * *****************************************5.4 ML High-level Steps *********************************************
    * **************************************************************************************************************/

  /*
  * The high-level steps generally depend on the type of a machine learning task and not so much on the
  * machine learning algorithms. For a given task, the same steps can be used with different machine learning
  * algorithms.
  *
  * Supervised machine learning task generally consists of the following high-level steps.
  * -- 1. Split data into training, validation, and test sets.
  * -- 2. Select the features for training a model.
  * -- 3. Fit a model on the training dataset using a supervised machine learning algorithm.
  * -- 4. Tune the hyperparameters using the validation dataset.
  * -- 5. Evaluate the model on a test dataset.
  * -- 6. Apply the model to new data.
  *
  * Unsupervised machine learning task generally consists of the following high-level steps.
  * -- 1. Select the feature variables.
  * -- 2. Fit a model using an unsupervised machine learning algorithm.
  * -- 3. Evaluate the model using the right evaluation metrics.
  * -- 4. Use the model.
  * */

  /******************************************************************************************************************
    * *****************************************5.5 ML in spark *****************************************************
    * **************************************************************************************************************/

  /*
  * Spark provides two machine learning libraries, MLlib and Spark ML (also known as the Pipelines API). These
  * libraries enable high-performance machine learning on large datasets. Unlike machine learning libraries that
  * can be used only with datasets that fit on a single machine, both MLlib and Spark ML are scalable. They make
  * it possible to utilize a multi-node cluster for machine learning.
  *
  * In addition, since Spark allows an application to cache a dataset in memory, machine learning applications
  * built with Spark ML or MLlib are fast.
  *
  * MLlib is the first machine learning library that shipped with Spark. It is more mature than Spark ML. Both
  * libraries provide higher-level abstractions for machine learning than the core Spark API
  * */

  /******************************************5.5.1 MLlib overview **********************************************/

  /* MLlib extends Spark for machine learning and statistical analysis. It provides a higher-level API than the
  * Spark core API for machine learning and statistical analysis. It comes prepackaged with commonly used
  * machine learning algorithms used for a variety of machine learning tasks. It also includes statistical utilities
  * for different statistical analysis.
  *
  * MLlib integrates with other Spark libraries such as Spark Streaming and Spark SQL. It can be used with both batch
  * and streaming data.
  *
  * Data preparation steps such as data cleansing and feature engineering becomes easier with the DataSet/Frame API
  * provided by Spark SQL. Generally, the raw data cannot be used directly with machine learning algorithms.
  * Features need to be extracted from the raw data.
  *
  * Statistical Utilities
  * MLlib provides classes and functions for common statistical analysis. It supports summary statistics,
  * correlations, stratified sampling, hypothesis testing, random data generation, and kernel density estimation.
  *
  *
  * Machine Learning Algorithms
  * MLlib can be used for common machine learning tasks such as regression, classification, clustering,
  * anomaly detection, dimensionality reduction, and recommendation. The list of algorithms that come
  * bundled with MLlib is ever growing. This section lists the algorithms shipped with MLlib at the time of 2016.
  *
  * - Regression and Classification
  * -- • Linear regression
  * -- • Logistic regression
  * -- • Support Vector Machine
  * -- • Naïve Bayes
  * -- • Decision tree
  * -- • Random forest
  * -- • Gradient-boosted trees
  * -- • Isotonic regression
  *
  * - Clustering
  * -- • K-means
  * -- • Streaming k-means
  * -- • Gaussian mixture
  * -- • Power iteration clustering (PIC)
  * -- • Latent Dirichlet allocation (LDA)
  *
  * - Dimensionality Reduction
  * -- • Principal component analysis (PCA)
  * -- • Singular value decomposition (SVD)
  *
  * - Feature Extraction and Transformation
  * -- • TF-IDF
  * -- • Word2Vec
  * -- • Standard Scaler
  * -- • Normalizer
  * -- • Chi-Squared feature selection
  * -- • Elementwise product
  *
  * - Frequent pattern mining
  * -- • FP-growth
  * -- • Association rules
  * -- • PrefixSpan
  *
  * - Recommendation
  * -- • Collaborative filtering with Alternating Least Squares (ALS)
  * */

  /******************************************5.5.2 MLlib API **********************************************/

  /******************************************5.5.3 ML API **********************************************/
}
}
