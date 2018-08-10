package org.pengfei.spark.ml.feature.extraction

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.sql.SparkSession
/*
* In the following code segment, we start with a set of sentences.
* We split each sentence into words using Tokenizer.
* For each sentence (bag of words), we use HashingTF to hash
* the sentence into a feature vector. We use IDF to rescale
* the feature vectors; this generally improves performance
* when using text as features. Our feature vectors could then
* be passed to a learning algorithm.
* */

/*
* The whole example are copied from https://spark.apache.org/docs/latest/ml-features.html#tf-idf
* */
object KeyWordExtraction {

  def main(args:Array[String])={
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = SparkSession.builder().
      master("local").
      appName("KeyWordExtraction").
      getOrCreate()

    val sentenceData=spark.createDataFrame(Seq(
      (0.0, "Hi I heard about Spark"),
      (0.0, "I wish Java could use case classes"),
      (1.0, "Logistic regression models are neat")

    )).toDF("label","sentence")

    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
    val wordsData = tokenizer.transform(sentenceData)

    val hashingTF=new HashingTF()
      .setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(2000)

    val featurizedData = hashingTF.transform(wordsData)
    featurizedData.show(10)
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)

    val rescaledData = idfModel.transform(featurizedData)
    rescaledData.select("label", "features").show()
  }

}
