package org.pengfei.spark.ml.feature.transformation
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg.Vectors

object StringFeatureTransformation {
/*
* As most of the machine learning model does not deal with string/text, we need to transform these string feature data
* into digit (int, float) index, and after the model training, we need to transfer the digit index back to string/text
*
* The org.apache.spark.ml.feature package provides many transformation fuctions such as
* - StringIndexer
* - IndexToString
* - OneHotEncoder
* - VectorIndexer
* */

  /***************************************************************************
  ********************** Basic StringIndexer ********************************
  ***************************************************************************/
  def main(args:Array[String])={
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = SparkSession.builder().
      master("local[2]"). //spark://10.70.3.48:7077 remote
      appName("StringFeatureTransformation").
      getOrCreate()
    //spark.conf.set("")
    import spark.implicits._

    val df1 = spark.createDataFrame(Seq(
      (0, "a"), (1, "b"), (2, "c"),
      (3, "a"), (4, "a"), (5, "c")
    )).toDF("id","category")

   // println(df1.show(5))
/*
* +---+--------+
| id|category|
+---+--------+
|  0|       a|
|  1|       b|
|  2|       c|
|  3|       a|
|  4|       a|
+---+--------+
* */

    // Build the StringIndexer model with input and output column
    val indexer = new StringIndexer().setInputCol("category").setOutputCol("categoryIndex")
    // Train the model with a dataframe
    val model = indexer.fit(df1)
    // After trainning, we can transform the dataframe with the model
    val indexed1 = model.transform(df1)
    // print(indexed1.show())

    /*
    * +---+--------+-------------+
| id|category|categoryIndex|
+---+--------+-------------+
|  0|       a|          0.0|
|  1|       b|          2.0|
|  2|       c|          1.0|
|  3|       a|          0.0|
|  4|       a|          0.0|
|  5|       c|          1.0|
+---+--------+-------------+
    *
    * */

    /******************Handle Unseen label error*******************/

    /*
    * Unseen label error happens when you build and train a model with a set of labels (e.g. a,b,c),
    * If we use this model to transform a data set which contains label (e.g. d) does not in the set of
    * labels.
    *
    * This error can be fixed by adding setHandleInvalid("skip"), but this will remove all the rows which
    * contains the label d*/

    val df2 = spark.createDataFrame(Seq(
      (0, "a"), (1, "b"), (2, "c"),
      (3, "a"), (4, "a"), (5, "d")
    )).toDF("id","category")

// The following two lines will generate unseen label errors
//    val indexedDf2= model.transform(df2)
//    println(indexedDf2.show())

    // The following line will skip all label which is not in the model
//    val indexed2 = model.setHandleInvalid("skip").transform(df2)
//    println(indexed2.show())

    /*To get all the rows, we need to train a new model with the new labels*/
    val model2=indexer.fit(df2)
    val indexed2=model2.transform(df2)
    //println(indexed2.show())

  /********************************************************************
  ************************ Index to String *********************************
  ************************************************************************* */

    val converter = new IndexToString().setInputCol("categoryIndex").setOutputCol("originalCategory")

    val originFeature=converter.transform(indexed2)
   // println(originFeature.show())


    /****************************************************************
      ************************ OneHotEncoder **************************
      ************************************************************* */
    val dfCatgegorical= spark.createDataFrame(Seq(
      (0, "a"), (1, "b"), (2, "c"), (3, "a"), (4, "a"),
      (5, "c"), (6, "d"), (7, "d"), (8, "d"), (9, "d"),
      (10, "e"), (11, "e"), (12, "e"), (13, "e"), (14, "e")
    )).toDF("id","category")

    val indexerOHE=new StringIndexer().setInputCol("category").setOutputCol("categoryIndex")
    val modelOHE=indexerOHE.fit(dfCatgegorical)
    val dfIndexed=modelOHE.transform(dfCatgegorical)
    print(dfIndexed.show())

    //.setDropLast(false)
    val encoderOHE=new OneHotEncoder().setInputCol("categoryIndex").setOutputCol("categoryVec")
    val dfEncoded = encoderOHE.transform(dfIndexed)
    print(dfEncoded.show())

    /*
    * With option setDropLast(false)
    * | id|category|categoryIndex|  categoryVec|
      +---+--------+-------------+-------------+
      |  0|       a|          2.0|(5,[2],[1.0])|
      |  1|       b|          4.0|(5,[4],[1.0])|
    *
    *
    * 5 -> 5 element in category
    * [2] -> category value is 2
    * [1.0] -> value presented
    *
    * Wihout option setDropLast(false)
    *
    * | id|category|categoryIndex|  categoryVec|
      +---+--------+-------------+-------------+
      |  0|       a|          2.0|(4,[2],[1.0])|
      |  1|       b|          4.0|    (4,[],[])|
    *
    * 4 -> 4 element only (5th element is dropped because it's last)
    * as b is the 5th element which is dropped, it has no index value
    * */

    /*******************************************************************************
      ***********************VectorIndexer ****************************************
      * ***************************************************************************/

    /*
    * StringIndexer only works for single string, if all the data has been already transformed into a vector
    * We will need vectorIndexer to do the job
    * */

    val vecData=Seq(
      Vectors.dense(-1.0, 1.0, 1.0),
      Vectors.dense(-1.0, 3.0, 1.0),
      Vectors.dense(0.0, 5.0, 1.0)
    )

    val vecDF=spark.createDataFrame(vecData.map(Tuple1.apply)).toDF("features")
    // The setMaxCategories(2) defienes that only the possible value is 2 or less will be considered as categorical features
    // This option will help vectorIndexer to identify categorical features
    val vecIndexer=new VectorIndexer().setInputCol("features").setOutputCol("indexed").setMaxCategories(2)
    val vecIndexerModel = vecIndexer.fit(vecDF)
    val categoricalFeatures: Set[Int] = vecIndexerModel.categoryMaps.keys.toSet
    println(s"Chose ${categoricalFeatures.size} categorical features: " + categoricalFeatures.mkString(", "))
    // With the above config, we have 2 categorical features, the column index is 0 and 2.
    // Because the column 1 has three values which is not met with setMaxCategories(2)
    // If we change the config to setMaxCategories(3) ,the output will be
    // Chose 3 categorical features: 0, 1, 2
    val indexedVecDF = vecIndexerModel.transform(vecDF)
/*
* +--------------+-------------+
  |      features|      indexed|
  +--------------+-------------+
  |[-1.0,1.0,1.0]|[1.0,1.0,0.0]|
  |[-1.0,3.0,1.0]|[1.0,3.0,0.0]|
  | [0.0,5.0,1.0]|[0.0,5.0,0.0]|
  +--------------+-------------+
* The column 0 has only two values (-1.0 and 0.0), the VectorIndexer index them as (1.0 and 0.0)
* The column 1 has 3 values -> not a categorical feautre -> no indexing
* The column 2 has only one value (1.0), the VectorIndexer index it as 0.0
* */
    print(indexedVecDF.show())


  }



}
