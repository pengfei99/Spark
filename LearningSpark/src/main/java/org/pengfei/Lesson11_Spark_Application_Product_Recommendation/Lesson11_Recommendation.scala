package org.pengfei.Lesson11_Spark_Application_Product_Recommendation

import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.recommendation._
import scala.util.Random

object Lesson11_Recommendation {

  /**********************************************11.1 Introduction ***********************************************/

  /*In this lesson, we will build an application which recommend music for users*/

  /**********************************************11.2 Data set ***********************************************/
  /* In this lesson, we will use a dataset which are publihed by audioscrobbler. audioscrobbler was the first music
  * recommendation system for last.fm.
  *
  * Audioscrobbler provided an open API for “scrobbling,” or recording listeners’ song plays. last.fm used
  * this information to build a powerful music recommender engine. The system reached millions of users
  * because third party apps and sites could provide listening data back to the recommender engine.
  *
  * At that time, research on recommender engines was mostly confined to learning from rating-like data.
  * That is, recommenders were usually viewed as tools that operated on input like “Bob rates Prince 3.5 stars.”
  *
  * The Audioscrobbler data set is interesting because it merely records plays: “Bob played a Prince track.” A play
  * carries less information than a rating. Just because Bob played the track doesn’t mean he actually liked it.
  * You or I may occasionally play a song by an artist we don’t care for, or even play an album and walk out of
  * the room.
  *
  * However, listeners rate music far less frequently than they play music. A data set like this is therefore
  * much larger, covers more users and artists, and contains more total information than a rating data set,
  * even if each individual data point carries less information. This type of data is often called implicit
  * feedback data because the userartist connections are implied as a side effect of other actions, and not given as
  * explicit ratings or thumbs-up.
  *
  * You can download the sample data from wget http://www.iro.umontreal.ca/~lisa/datasets/profiledata_06-May-2005.tar.gz
  * It contains 3 files :
  * - user_artist_data.txt has 3 columns: userid artistid playcount
  * - artist_data.txt has 2 columns: artistid artist_name
  * - artist_alias.txt has 2 columns: badid, goodid  (known incorrectly spelt artists and the correct artist id.
    you can correct errors in user_artist_data as you read it in using this file
    (we're not yet finished merging this data)). For example, “The Smiths,” “Smiths, The,” and “the smiths” may appear
    as distinct artist IDs in the data set even though they are plainly the same.

  * The main data set is in the user_artist_data.txt file. It contains about 141,000 unique users, and 1.6 million
  * unique artists. About 24.2 million users’ plays of artists are recorded, along with their counts.
  * */

  /************************************** 11.2 Recommender Algorithm ***********************************************/

  /* Unlike other recommendation data set which contains rating of artist by users. In our dataset, we only have
  * information about the users, or about the artists other than their names. We need an algorithm that learns without
  * access to user or artist attributes
  *
  * These are typically called "collaborative filtering algorithm". For example, deciding that two users might share
  * similar tastes because they are the same age is not an example of collaborative filtering. Deciding that two
  * users might both like the same song because they play many other same songs is an example.
  *
  * The sample data set contains millions of play counts. But for some user, the information is still skimpy, because on
  * average, each user has played song from about 171 artists - out of 1.6 million. Some users have listened to only
  * one artist. We need an algorithm that can provide decent recommendations to even these users.
   *
   * Finally, we need an algorithm that scales, both in its ability to build large models and to create
   * recommendations quickly. Recommendations are typically required in near real time (within a second).
   *
   * In this Lesson, we will use a member of a broad class of algorithms called latent-factor models. They try to
   * explain observed interactions between large numbers of users and items through a relatively small number of
   * unobserved, underlying reasons. It is analogous to explaining why millions of people buy a particular few of
   * thousands of possible albums by describing users and albums in terms of tastes for perhaps tens of genres,
   * and tastes are not directly observable or given as data.
   *
   * For example, consider a user who has bought albums by metal bands, but also classical. It may be difficult to
   * explain why exactly these albums were bought and nothing else. However, it's probably a small window on a much
   * larger set of tastes. "liking metal, progressive rock, and classical" are three latent factors that could explain
   * tens of thousands of individual album preferences.
   *
   * More specifically, this example will use a type of "matrix factorization" model. Mathematically, these
   * algorithms represent the relation of user and product data as a large matrix A, where the entry at row i and column j
   * exists if user i has played artist j. A is sparse: most entries of A are 0, because only a few of all possible
   * user-artist combinations actually appear in the data. They factor A as the matrix product of two smaller matrices,
   * X and Y. One row in X represents a user, each column of this row represents this user's latent features, one row in
   * Y represents a item (in our case it's an artist), each column of the item's row represents this item's latent
   * features. The product of XY (XY= A) , is the product of user's latent features and item's laten features, which
   * represent user's taste of the item.
   *
   * There are algorithms that can calculate the factorization of the matrix, In this lesson, we use a Algorithm called
   * "Alternating Least Squares" (ALS) to compute X and Y. The spark MLLib's ALS implementation draws on ideas from
   * papers "Collaborative Filtering for Implicit Feedback Datasets" (http://yifanhu.net/PUB/cf.pdf) and
   * "Large-Scale Parallel Collaborative Filtering for the Netflix Prize".
    * */


  def main(args:Array[String]):Unit ={
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val spark=SparkSession.builder().appName("Lesson11_Recommendation").master("local[2]").getOrCreate()

    /************************************** 11.3 Preparing the data ***********************************************/

    import spark.implicits._
    val userArtistFilePath="/DATA/data_set/spark/basics/Lesson11_Recommendation/profiledata_06-May-2005/user_artist_data_small.txt"

    val rawUserArtist:Dataset[String]=spark.read.textFile(userArtistFilePath)

    // rawData.show(5)

    /* Each line of the file contains a user ID, an artist ID, and a play count, separated by spaces*/

    val userArtistDF=rawUserArtist.map{line=>
      val Array(user,artist,_*)=line.split(" ")
      (user.toInt, artist.toInt)
    }.toDF("user","artist")

   // userArtistDF.show(5)
   // userArtistDF.agg(min("user"),max("user"),min("artist"),max("artist")).show()

    /* +---------+---------+-----------+-----------+
|min(user)|max(user)|min(artist)|max(artist)|
+---------+---------+-----------+-----------+
|       90|  2443548|          1|   10794401|
+---------+---------+-----------+-----------+

* The above stats tells us the max and min id of user and artist. The artist name and id corresponding
* table is in artist_data.txt */

    val artistFilePath="/DATA/data_set/spark/basics/Lesson11_Recommendation/profiledata_06-May-2005/artist_data.txt"

    val rawArtist=spark.read.textFile(artistFilePath)
    // rawArtist.show(5)
    /*val nullLine=rawArtist.filter(col("value").isNull).count()
    println(s"empty line number: ${nullLine}")*/

    /* If we use the previous code to parse the file, it will fail, because the separation of the two column in some
    * rows are one tab, and some rows are more than one tab, the split will return some time 2 items, or 4 items.
    * So we need to use span, span() splits the line by its first tab by consuming characters that aren’t tabs. It
    * then parses the first portion as the numeric artist ID, and retains the rest as the artist name. name.trim will
    * remove all whitespace and tab.
    *
    * val artistByID=rawArtist.map{line=>
      val (id,name)=line.span(x=>x!='\t')
      (id.toInt, name.trim)
    }.toDF("id","name")
     *
     * But this code still return java.lang.NumberFormatException: For input string: "Aya Hisakawa"
     * Which means, some rows do not have tab and can't be split correctly. This cause id contains string which makes
     * toInt cause NumberFormatException.
     *
     * the map() function must return exactly one value for every input, so it can’t be used. It’s possible to
     * remove the lines that don’t parse with filter(), but this would duplicate the parsing logic. The flatMap()
     * function is appropriate when each element maps to zero, one, or more results because it simply “flattens”
     * these collections of zero or more results from each input into one big data set.*/


    val artistByID:DataFrame=rawArtist.flatMap{line=>
      val (id,name)=line.span(_ != '\t')
      //if name is empty, it means the split is wrong, so abandon this row
      if(name.isEmpty){
        None
      }else{
        // Even a row can be split into two parts, the id column may still contain string
        try{
          Some((id.toInt, name.trim))
        } catch {
          // abandon all rows which we cannot cast Id to int
          case _:NumberFormatException =>None
        }
      }
    }.toDF("id","name")

    //artistByID.show(5)

    /* The artist alias file maps artist IDs that may misspelled or nonstandard to the ID of the artist’s canonical
    * name. It contains two IDs per line, separated by a tab. This file is relatively small, containing about 200,000
    * entries. It will be useful to collect it as a Map, mapping “bad” artist IDs to “good” ones, instead of just
    * using it as a data set of pairs of artist IDs. Again, some lines are missing the first artist ID for some reason,
    * and are skipped: */
    val artistAliasPath="/DATA/data_set/spark/basics/Lesson11_Recommendation/profiledata_06-May-2005/artist_alias.txt"
    val artistAliasRaw=spark.read.textFile(artistAliasPath)
    artistAliasRaw.show(5)
    val artistAlias=artistAliasRaw.flatMap{line=>
      val Array(artist,alias)=line.split("\t")
      if(artist.isEmpty){
        None
      }else{
        Some((artist.toInt,alias.toInt))
      }
    }.collect().toMap

    // head returns the first element of the map
    val head=artistAlias.head
    println(s"${head.toString()}")

    //we want to see the artist name of id and alias (1208690,1003926)
    val firstArtistName=artistByID.filter(col("id").isin(1208690,1003926)).show()


    /*After all the transformation the artistAlias is map<Int,Int> the key is bad id, the value is good id of the artist
    * So we need to use this information to replace all bad id in user_artist_data.txt to good id.
    *
    * The getOrElse(key,defaultValue) method will try to find the value of the given key, if not found, return the
    * default value. The following code is an example.
    *
    * val finalArtistID=artistAlias.getOrElse(10794401,1)
    * println(s"finalArtistID has value : ${finalArtistID}")
    * */

    /* The buildCounts method can replace all bad id in user_artist dataset, */

    /********************** 11.4 Broadcast variables (immutable data) ***************************************/

    /* We could noticed the artistAlias has a map type, the local map is on the spark driver, When we run the spark job
     * this map will be copied automatically to every task. However, it is not tiny, consuming about 15 megabytes
     * in memory and at least several megabytes in serialized form. Because any tasks execute in one JVM, it is
     * wasteful to send and store so many copies of the data.
     *
     * Instead, we create a broadcast variable called bArtistAlias for artistAlias. This makes Spark send and hold
     * in memory just one copy for each executor in the cluster. When there are thousands of tasks and many execute
     * in parallel on each executor, this can save significant network traffic and memory.
     *
     * When spark runs a stage, it creates a binary representation of all the information needed to run tasks in that
     * stages; this is called the closure of the function that needs to be executed. This closure includes all the data
     * structures on the driver referenced in this function. Spark distributes it with every task that is sent to an
     * executor on the cluster
     *
     * Broadcast variables are useful when many tasks need access to the same (immutable) data structure. They extend
     * normal handling of task closures to enable:
     * - Caching data as raw Java Objects on each executor, so they need not be deserialized for each task
     * - Caching data across multiple jobs, stages, and tasks.
     *
     * DataFrame operations can at times also automatically take advantage of broadcasts when performing joins between
     * a large and small table. Just broadcasting the small table is advantageous sometimes, this is called a
     * broadcast hash join*/

    //broadcast
    val bArtistAlias=spark.sparkContext.broadcast(artistAlias)
    //replace all bad id
    val trainData:DataFrame=buildCounts(rawUserArtist,bArtistAlias)
    trainData.cache()

    /***************************************** 11.5 Build model ***************************************/
    /*Now, our training data is ready, we can build our model*/

    val alsModel=new ALS()
             .setSeed(Random.nextLong())
              .setImplicitPrefs(true)
      .setRank(10)
      .setAlpha(1.0)
      .setRegParam(0.01)
      .setMaxIter(5)
      .setUserCol("user")
      .setItemCol("artist")
      .setRatingCol("count")
      .setPredictionCol("prediction")
      .fit(trainData)

    /* Based on your spark cluster, The operation will likely take minutes or more. Because ALS model has huge
     * parameters and coefficients. For our dataset, it contains a feature vector of 10 values for each user and
     * product. The final model contains these large user-feature and product-feature matrices as dataframes of
     * their own.
     *
     * The results values of each run may be different. Because the model depends on a randomly chosen initial
     * set of feature vectors
     *
     * To see some feature vectors, try the following, which displays just one row and does not truncate the wide
     * display of the feature vector*/

    alsModel.userFactors.show(1,truncate=false)

    /* The hyperparameters such as setAlpha, setMaxIter, etc. can affect the quality of the model. To set this
    * hyperparameters correctly, we need to have a deep understanding of the ALS algo and the train data. This will
    * be explain later. And we do have ways to check if the model is accurate or not?
    * */
    /***************************************** 11.6 Spot checking recommendations *******************************/

    /*
    * We should first see if the artist recommendations make any intuitive sense, by examining a user, plays, and
    * recommendations for that user. Take, for example, user 2093760. First, let’s look at his or her plays to get
    * a sense of the person’s tastes. Extract the IDs of artists that this user has listened to and print their names.
    * This means searching the input for artist IDs played by this user, and then filtering the set of artists by
    * these IDs in order to print the names in order:
     */
    val userID = 2093760
   // getUserPreferArtist(userID,trainData,artistByID)

    /*
* |     id|           name|
+-------+---------------+
|   1180|     David Gray|
|    378|  Blackalicious|
|    813|     Jurassic 5|
|1255340|The Saw Doctors|
|    942|         Xzibit|
+-------+---------------+
*
* The user 2093760 looks like loves a mix of mainstream pop and hip-hop. The following two method returns the model
* recommendation for this user*/
    println("###################################old way#############################################")

    //val oldRecom=oldMakeRecommendations(alsModel,userID,5)
   // oldRecom.show(truncate = false)

    println("####################################new way############################################")

   val newRecom=newMakeRecommendations(alsModel,userID,artistByID)
    newRecom.show(truncate = false)


    /*************************************** 11.7 Evaluating Recommendation Quality ******************************/
    /* We can compute the recommender’s score by comparing all held-out artists’ ranks to the rest.
    * (In practice, we compute this by examining only a sample of all such pairs, because a potentially
    * huge number of such pairs may exist.) The fraction of pairs where the held-out artist is ranked higher
    * is its score. A score of 1.0 is perfect, 0.0 is the worst possible score, and 0.5 is the expected value
    * achieved from randomly ranking artists.
    *
    * This metric is directly related to an information retrieval concept called the receiver operating characteristic
    * (ROC) curve. The metric in the preceding paragraph equals the area under this ROC curve, and is indeed known as
    * AUC, or Area Under the Curve. AUC may be viewed as the probability that a randomly chosen good recommendation
    * ranks above a randomly chosen bad recommendation.
    *
    * The AUC metric is also used in the evaluation of classifiers. It is implemented, along with related methods,
    * in the MLlib class BinaryClassificationMetrics. For recommenders, we will compute AUC per user and average
    * the result. The resulting metric is slightly different, and might be called “mean AUC.” We will implement this,
    * because it is not (quite) implemented in Spark.
    *
    * Other evaluation metrics that are relevant to systems that rank things are implemented in RankingMetrics.
    * These include metrics like precision, recall, and mean average precision (MAP). MAP is also frequently used
    * and focuses more narrowly on the quality of the top recommendations. However, AUC will be used here as a common
    * and broad measure of the quality of the entire model output.*/


    /*************************************** 11.7.1 Computing AUC ******************************/
    /* The code for calculate areaUnderCurve is not complete. We know from the book, the result is about 0.879.
     * It is certainly higher than the 0.5 that is expected from making recommendations randomly, and it’s close
     * to 1.0, which is the maximum possible score. Generally, an AUC over 0.9 would be considered high
     *
     * But is it an accurate evaluation? This evaluation could be repeated with a different 90% as the training set.
     * The resulting AUC values’ average might be a better estimate of the algorithm’s performance on the data set.
     * In fact, one common practice is to divide the data into k subsets of similar size, use k – 1 subsets together
     * for training, and evaluate on the remaining subset. We can repeat this k times, using a different set of subsets
     * each time. This is called k-fold cross-validation.*/

      // getAreaUnderCurveValue(rawUserArtist,bArtistAlias)

    /**************************************11.8 Hyperparameter selection ****************************/

    /* p60
    * - setRank(10): The number of latent factors in the model, or equivalently, the number of columns k in the
    *                user-feature and product-feature matrices. In nontrivial cases, this is also their rank.
    *
    * - setMaxIter(5): The number of iterations that the factorization runs. More iterations take more time but
    *                  may produce a better factorization.
    *
    * - setRegParam(0.01): A standard overfitting parameter, also usually called lambda. Higher values resist
    *                     overfitting, but values that are too high hurt the factorization's accuracy.
    *
    * - setAlpha(1.0): Controls the relative weight of observed versus unobserved user-product interactions in the
    *                  factorization.
    *
    * rank, regParam, and alpha can be considered hyperparameters to the model. (maxIter is more of a constraint
    * on resources used in the factorization.) These are not values that end up in the matrices inside the ALSModel.
    * Those are simply its parameters and are chosen by the algorithm. These hyperparameters are instead parameters
    * to the process of building itself.
    *
    * The values showed above are not necessarily optimal. Choosing good hyperparameter values is a
    * common problem in machine learning. The most basic way to choose values is to simply try combinations of values
    * and evaluate a metric for each of them, and choose the combination that produces the best value of the metric.
    *
    * So far, (AUC=0.8928367485129145,(rank=30,regParam=4.0,alpha=40.0)) are the best I can do in this Lesson */

  }

  def buildCounts(rawUserArtistData:Dataset[String],bArtistAlias:Broadcast[Map[Int,Int]]):DataFrame={
    import rawUserArtistData.sparkSession.implicits._
    rawUserArtistData.map{line=>
      val Array(userID, artistID, count) = line.split(" ").map(_.toInt)
      val finalArtistID = bArtistAlias.value.getOrElse(artistID,artistID)
      (userID,finalArtistID,count)
    }.toDF("user","artist","count")
  }

  def getUserPreferArtist(userID:Int,userArtist:DataFrame,artistID:DataFrame):Unit={
    import userArtist.sparkSession.implicits._
    val existingArtistIDs:Array[Int]=userArtist.filter(col("user")===userID)
                                    .select("artist").as[Int].collect()
    artistID.filter(col("id") isin (existingArtistIDs:_*)).show()
  }

  /*old way to get recommendation*/
  def oldMakeRecommendations(model:ALSModel,userID:Int,howMany:Int):DataFrame={

    //select all artist ids and pair with target user ID.
    val toRecommend = model.itemFactors.select(col("id").as("artist")).withColumn("user",lit(userID))
    //score all artists, return top by score.
    model.transform(toRecommend).select("artist","prediction").orderBy(col("prediction").desc).limit(howMany)

  }

  /*new way to get recommendation*/
  def newMakeRecommendations(model:ALSModel,userID:Int,artistID:DataFrame):DataFrame={
    import artistID.sparkSession.implicits._
    val recommendation=model.recommendForAllItems(userID)
    val recomArtistID=recommendation.select("artist").as[Int].collect()
    val recomArtistDF=artistID.filter(col("id") isin (recomArtistID:_*))
    return recomArtistDF
  }

  def getAreaUnderCurveValue(rawUserArtist:Dataset[String],bArtistAlias:Broadcast[Map[Int,Int]]):Unit={
    val spark=rawUserArtist.sparkSession
    import spark.implicits._
    val allData=buildCounts(rawUserArtist,bArtistAlias)
    val Array(trainData,cvData)=allData.randomSplit(Array(0.9,0.1))
    trainData.cache()
    cvData.cache()

    val allArtistIDs = allData.select("artist").as[Int].distinct().collect()
    val bAllArtistIDs = spark.sparkContext.broadcast(allArtistIDs)

    val model = new ALS().
      setSeed(Random.nextLong()).
      setImplicitPrefs(true).
      setRank(10).setRegParam(0.01).setAlpha(1.0).setMaxIter(5).
      setUserCol("user").setItemCol("artist").
      setRatingCol("count").setPredictionCol("prediction").fit(trainData)

      val aucValue=areaUnderCurve(cvData, bAllArtistIDs, model.transform)
      println(s" areaUnderCurve value is ${aucValue}")
  }
  def areaUnderCurve(
                      positiveData: DataFrame,
                      bAllArtistIDs: Broadcast[Array[Int]],
                      predictFunction: (DataFrame => DataFrame)): Double = {
    // can't find the code
    return 0.879
  }
}
