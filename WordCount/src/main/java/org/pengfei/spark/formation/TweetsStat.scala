package org.pengfei.spark.formation

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr

object TweetsStat {
  def main(args:Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = SparkSession.builder().
      master("local[2]"). //spark://10.70.3.48:7077 remote
      appName("TweetsStat").
      getOrCreate()
    //spark.conf.set("")
    import spark.implicits._

    val inputFile = "file:////home/pliu/Downloads/tweets.csv"
    val jsonInputFile = "file:///tmp/test/output/total"

    val tweetDF=spark.read.json(jsonInputFile)

    val textFile=spark.read.text(inputFile).as[String]

  /* counts words in all text */
    //val counts=textFile.flatMap(line=>line.split(" ")).groupByKey(_.toLowerCase).count()

    //counts.show()
    //counts.filter($"value".contains("trump")).show(10)
    //textFile.show(10)
     tweetDF.show(10)
    print(tweetDF.count())
    val trumpTweets= tweetDF.filter($"text".contains("Trump")).select($"text")
    //print(trumpTweets.count())
    trumpTweets.write.format("csv").save("file:///tmp/test/output/trump")
    /* counts words in each line and calculate min max and mean*/
    spark.udf.register("lineWordCount", (arg1: String)=>lineWordCount(arg1))
    val textLengthDF=textFile.withColumn("value",expr("lineWordCount(value)"))
    /*textLengthDF.show(5)
    textLengthDF.describe().show()*/


  }

  def lineWordCount(text: String): Long={

    val word=text.split(" ").map(_.toLowerCase).groupBy(identity).mapValues(_.size)
    val counts=word.foldLeft(0){case (a,(k,v))=>a+v}
    /* print(word)
     print(counts)*/
    return counts

  }
}
