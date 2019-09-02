package org.pengfei.Lesson06_Spark_Streaming

import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import twitter4j.Status

object Lesson06_4_Exo1 {
/* In this exercise, let’s develop a complete Spark Streaming application so that you can see how the classes
* and methods discussed in earlier sections come together in an application. You will create an application
* that shows trending Twitter hashtags.
*
* In tweeter, we use # to mark a topic. People add the hashtag symbol before a word to categorize a tweet. A tweet may
* contain zero or more hashtagged words.
*
* Twitter provides access to its global stream of tweets through a streaming API. You can learn about this
* API on Twitter’s web site at https://dev.twitter.com/streaming/overview.
*
* To get access to the tweets through Twitter’s streaming API, you need to create a Twitter account and
* register your application. An application needs four pieces of authentication information to connect to
* Twitter’s streaming API: consumer key, consumer secret, access token, and access token secret. You can
* obtain these from Twitter. If you have a Twitter account, sign-in or create a new account. After signing-in,
* register your application at https://apps.twitter.com to get all the authentication credentials.
*
* Let’s create a Spark Streaming application that tracks hash tagged words and shows the ones that are
* trending or gaining popularity. Application source code is shown next, followed by code explanation.*/


  def main(args:Array[String])={
//Twitter connection info
    val consumerKey="9F2cDP6mBO001MJtFyLybWGqT"
    val consumerSecret="czoiPDtFaNrUboAQEPPXpb9EMwbTgW7WB6bZYxpBljy2Ozqrqc"
    val accessToken="207529473-WHBQShBPtIKYDS5867aDmyvOwmSN7JueREPgDPT9"
    val accessTokenSecret="P6A1I5lCbJWEAL1yFO1T1kP9OEQnm3nOcyGfYrZkUd1la"


    // Set the system properties so that Twitter4j library used by twitter stream
    // can use them to generate OAuth credentials
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)


    // spark streaming info
    val batchInterval = 10
    // The threshold variable is used to treat late data. See Lesson6_3
    val minThreshold = 20
    // path for streaming context checkpoint
    val checkPointPath="/tmp/spark/check-point"

    //build streaming context with a spark session
    val spark=SparkSession.builder().appName("Lesson6_4_Exo1_TwitterPoplularHashTag").master("locat[2]").getOrCreate()
    val ssc=new StreamingContext(spark.sparkContext,Seconds(batchInterval))

    ssc.checkpoint(checkPointPath)

    //tweeter filter
    val filters = Array("Trump","China")

    //Get tweeter dstream with a filter
    val tweetDStream = TwitterUtils.createStream(ssc, None, filters)

    //We can also filter the tweets by language
    val language="en"
    val tweetsFilteredByLang = tweetDStream.filter{tweet=>tweet.getLang()==language}

    //Get tweet text and split to word list
    val statuses=tweetsFilteredByLang.map(_.getText)
    val words=statuses.flatMap{status=>status.split("""\s+""")}

    //get hashTags
    val hashTags=words.filter{word=>word.startsWith("#")}
    val hashTagPairs=hashTags.map{tag=>(tag,1)}
    val tagsWithCounts=hashTagPairs.updateStateByKey(
      (counts:Seq[Int],prevCount:Option[Int])=>prevCount.map{c=>c+counts.sum}.orElse{Some(counts.sum)}
    )
  }
}
