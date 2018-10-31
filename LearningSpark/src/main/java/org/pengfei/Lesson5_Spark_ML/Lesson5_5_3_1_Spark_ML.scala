package org.pengfei.Lesson5_Spark_ML

object Lesson5_5_3_1_Spark_ML {

  /*********************************************************************************************************
    * **********************************5.5.3 Spark Machine Learning pipelines API(Spark ML) ***************************
    * ********************************************************************************************************/

/*
* Spark ML is another machine learning library that runs on top of Spark. It is a relatively newer library than
* MLlib. It became available starting with Apache Spark version 1.2. It is also referred to as the Spark Machine
* Learning Pipelines API.
*
* Spark ML provides a higher-level abstraction than MLlib for creating machine learning workflows or
* pipelines. It enables users to quickly assemble and tune machine learning pipelines. It makes it easy to
* create a pipeline for training a model, tuning a model using cross-validation, and evaluating a model with
* different metrics.
*
* Many classes and singleton objects provided by the MLlib library are also provided by the Spark ML
* library. In fact, classes and objects related to machine learning algorithms and models have the same names
* in both the libraries. The classes and singleton objects provided by the Spark ML library are available under
* the org.apache.spark.ml package.
*
* Generally, a machine learning task consists of the following steps:
* - 1. Read data.
* - 2. Preprocess or prepare data for processing.
* - 3. Extract features.
* - 4. Split data for training, validation, and testing.
* - 5. Train a model with a training dataset.
* - 6. Tune a model using cross-validation techniques.
* - 7. Evaluate a model over a test dataset.
* - 8. Deploy a model.
* */

  /*********************************************************************************************************
    * **********************************5.5.3.1 Spark ML datatype *****************************************
    * ********************************************************************************************************/

  /*
  * Spark ML uses DataFrame as the primary data abstraction. Unlike in MLlib, the machine learning algorithms
  * and models provided by Spark ML operate on DataFrames.
  *
  * As discussed in Lesson 4 spark sql, the DataFrame API provides a higher-level abstraction than the RDD API
  * for representing structured data. It supports a flexible schema that allows named columns of different data
  * types. For example, a DataFrame can have different columns storing raw data, feature vectors, actual label,
  * and predicted label. In addition, the DataFrame API supports a wide variety of data sources.
  *
  * Compared to the RDD API, the DataFrame API also makes data preprocessing and feature extraction
  * or feature engineering easier. Data cleansing and feature engineering are generally required before a model
  * can be fitted on a dataset. These activities constitute the majority of the work involved in a machine learning
  * task. The DataFrame API makes it easy to generate a new column from an existing one and add it to the
  * source DataFrame.
  *
  */
  /*********************************************************************************************************
    * **********************************5.5.3.2 Spark ML key features *****************************************
    * ********************************************************************************************************/
  /* Transformer
  * A Transformer generates a new DataFrame from an existing DataFrame. It implements a method named
  * transform, which takes a DataFrame as input and returns a new DataFrame by appending one or more new
  * columns to the input DataFrame. A DataFrame is an immutable data structure, so a transformer does not
  * modify the input DataFrame. Instead, it returns a new DataFrame, which includes both the columns in the
  * input DataFrame and the new columns.
  *
  * Spark ML provides two types of transformers:
  * - feature transformer
  * - machine learning model.
  *
  * Feature Transformer
  * A feature transformer creates one or more new columns by applying a transformation to a column in the
  * input dataset and returns a new DataFrame with the new columns appended. For example, if the input
  * dataset has a column containing sentences, a feature transformer can be used to split the sentences into
  * words and create a new column that stores the words in an array.
  *
  * Model
  * A model represents a machine learning model. It takes a DataFrame as input and outputs a new DataFrame
  * with predicted labels for each input feature Vector. The input dataset must have a column containing feature
  * Vectors. A model reads the column containing feature Vectors, predicts a label for each feature Vector, and
  * returns a new DataFrame with predicted labels appended as a new column.
  *
  * Estimator
  * An Estimator trains or fits a machine learning model on a training dataset. It represents a machine learning
  * algorithm. It implements a method named fit, which takes a DataFrame as argument and returns a
  * machine learning model.
  *
  * An example of an Estimator is the LinearRegression class. Its fit method returns an instance of the
  * LinearRegressionModel class.
  *
  * Pipeline
  * A Pipeline connects multiple transformers and estimators in a specified sequence to form a machine
  * learning workflow. Conceptually, it chains together the data preprocessing, feature extraction, and model
  * training steps in a machine learning workflow.
  *
  * A Pipeline consists of a sequence of stages, where each stage is either a Transformer or an Estimator.
  * It runs these stages in the order they are specified.
  * A Pipeline itself is also an Estimator. It implements a fit method, which takes a DataFrame as
  * argument and passes it through the pipeline stages. The input DataFrame is transformed by each stage.
  *
  * The fit method returns a PipelineModel, which is a Transformer.
  * A Pipeline’s fit method calls the transform method of each Transformer and fit method of each
  * Estimator in the same order as they are specified when a Pipeline is created. Each Transformer takes a
  * DataFrame as input and returns a new DataFrame, which becomes the input for the next stage in the
  * Pipeline. If a stage is an Estimator, its fit method is called to train a model. The returned model, which is a
  * Transformer, is used to transform the output from previous stage to produce input for the next stage.
  *
  * PipelineModel
  * A PipelineModel represents a fitted pipeline. It is generated by the fit method of a Pipeline. It has the same
  * stages as the Pipeline that generated it, except for the Estimators, which are replaced by models trained by
  * those estimators. In other words, all the Estimators are replaced by Transformers.
  *
  * Unlike a Pipeline, which is an Estimator, a PipelineModel is a Transformer. It can be applied to a dataset
  * to generate predictions for each observation. In fact, a PipelineModel is a sequence of Transformers. When
  * the transform method of a PipelineModel is called with a DataFrame, it calls the transform method of
  * each Transformer in sequence. Each Transformer’s transform method outputs a new DataFrame, which
  * becomes the input for the next Transformer in the sequence.
  *
  *
  * Evaluator
  * An Evaluator evaluates the predictive performance or effectiveness of a model. It provides a method named
  * evaluate, which takes a DataFrame as input and returns a scalar metric. The input DataFrame passed as
  * argument to the evaluate method must have columns named label and prediction.
  *
  * Grid Search
  * The performance or quality of a machine learning model depends on the hyperparameters provided to a
  * machine learning algorithm during model training. For example, the effectiveness of a model trained with
  * the logistic regression algorithm depends on the step size and number of gradient descent iterations.
  *
  * Unfortunately, it is difficult to pick the right combination of hyperparameters for training the
  * best model. One of the techniques for finding the best hyperparameters is to do a grid search over a
  * hyperparameter space. In a grid search, models are trained with each combination of hyperparameters from
  * a specified subset of the hyperparameter space.
  *
  * For example, consider a training algorithm that requires two real-valued hyperparameters: p1 and p2.
  * Rather than guessing the best values for p1 and p2, we can do a grid search over p1 values 0.01, 0.1, and 1,
  * and p2 values 20, 40, and 60. This results in nine different combinations of p1 and p2. A model is trained and
  * evaluated with each combination of p1 and p2. The combination that trains a model with the best evaluation
  * metric is selected.
  *
  * Grid search is expensive, but it is a better approach for hyperparameter tuning than guessing the
  * optimal value for each hyperparameter. Generally, it is a required step to find a model that performs well.
  *
  * CrossValidator
  * A CrossValidator finds the best combination of hyperparameter values for training the optimal model for a
  * machine learning task. It requires an Estimator, an Evaluator and a grid of hyperparameters.
  *
  * A CrossValidator uses k-fold cross-validation and grid search for hyperparameter and model tuning.
  * It splits a training dataset into k-folds, where k is a number specified by a user. For example, if k is 10, a
  * CrossValidator will generate 10 pairs of training and test dataset from the input dataset. In each pair, 90% of
  * the data is reserved for training and remaining 10% is held-out for testing.
  * Next, it generates all the combinations of the hyperparameters from user-specified sets of different
  * hyperparameters. For each combination, it trains a model with a training dataset using an Estimator and
  * evaluates the generated model over a test dataset using an Evaluator. It repeats this step for all the k-pairs of
  * training and test datasets, and calculates the average of the specified evaluation metric for each pair.
  * The hyperparameters that produce the model with the best averaged evaluation metric are selected
  * as the best hyperparameters. Finally, a CrossValidator trains a model over the entire dataset using the best
  * hyperparameters.
  * Note that using a CrossValidator can be very expensive since it tries every combination of the
  * hyperparameters from the specified parameter grid. However, it is a well-established method for choosing
  * optimal hyperparameter values. It is statistically a better method than heuristic hand-tuning.
  *
  *
  * */

}
