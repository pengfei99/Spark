package org.pengfei.Lesson13_Anomaly_Detection

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.linalg.{Vectors,Vector}
import scala.util.Random

/*
* The source code of Advanced Analytics with Spark can be found https://github.com/sryza/aas
* */
object Lesson13_Anomaly_Detection {

  /****************************************************************************************************************
    * ***************************************13.1 Introduction **************************************************
    * ***********************************************************************************************************/

  /* In this lesson, we will use a unsupervised learning technique (e.g. clustering), Clustering can identify
   * groups inside a data set. Consider the problem of dividing up an ecommerce site’s customers by their shopping
   * habits and tastes. The input features are their purchases, clicks, demographic information, and more.
   * The output should be groupings of customers: perhaps one group will represent fashion-conscious buyers,
   * another will turn out to correspond to pricesensitive bargain hunters, and so on.
   *
   * If you were asked to determine this target label for each new customer, you would quickly run into a problem
   * in applying a supervised learning technique like a classifier: you don’t know a priori who should be considered
   * fashion-conscious, for example. In fact, you’re not even sure if “fashion-conscious” is a meaningful grouping of
   * the site’s customers to begin with!
   *
   * Fortunately, unsupervised learning techniques can help. These techniques do not learn to predict a target
   * value, because none is available. They can, however, learn structure in data and find groupings of similar
   * inputs, or learn what types of input are likely to occur and what types are not.
   * */

  /******************************************************************************************************************
    * ***************************************13.2 Anomaly Detection ************************************************
    * **************************************************************************************************************/

  /* The inherent problem of anomaly detection is, as its name implies, that of finding unusual things. If we already
  * knew what “anomalous” meant for a data set, we could easily detect anomalies in the data with supervised learning.
  * An algorithm would receive inputs labeled “normal” and “anomaly”, and learn to distinguish the two.
  *
  * However, the nature of anomalies is that they are unknown unknowns. Put another way, an anomaly that has been
  * observed and understood is no longer an anomaly. Anomaly detection is often used to find fraud, detect network
  * attacks, or discover problems in servers or other sensor-equipped machinery. In these cases, it’s important
  * to be able to find new types of anomalies that have never been seen before—new forms of fraud, intrusions,
  * and failure modes for servers.
  *
  * Unsupervised learning techniques are useful in these cases because they can learn what input data normally
  * looks like, and therefore detect when new data is unlike past data. Such new data is not necessarily attacks
  * or fraud; it is simply unusual, and therefore, worth further investigation.
  * */

  /******************************************************************************************************************
    * **************************************13.3 K-means clustering *************************************************
    * ***************************************************************************************************************/

  /* Clustering is the best-known type of unsupervised learning. Clustering algorithms try to find natural groupings
  * in data. Data points that are like one another but unlike others are likely to represent a meaningful grouping,
  * so clustering algorithms try to put such data into the same cluster.
  *
  * K-means clustering may be the most widely used clustering algorithm. It attempts to detect k clusters in a data
  * set, where k is given by the data scientist. k is a hyperparameter of the model, and the right value will depend
  * on the data set. In fact, choosing a good value for k will be a central plot point in this chapter.
  *
  * What does “like” mean when the data set contains information like customer activity? Or transactions? K-means
  * requires a notion of distance between data points. It is common to use simple Euclidean distance to measure
  * distance between data points with K-means, and as it happens, this is the only distance function supported by
  * Spark MLlib as of this writing. The Euclidean distance is defined for data points whose features are all numeric.
  * “Like” points are those whose intervening distance is small.
  *
  * To K-means, a cluster is simply a point: the center of all the points that make up the cluster. These are,
  * in fact, just feature vectors containing all numeric features, and can be called vectors. However, it may be
  * more intuitive to think of them as points here, because they are treated as points in a Euclidean space.
  *
  * This center is called the cluster centroid, and is the arithmetic mean of the points—hence the name K-means.
  * To start, the algorithm picks some data points as the initial cluster centroids. Then each data point is assigned
  * to the nearest centroid. Then for each cluster, a new cluster centroid is computed as the mean of the data points
  * just assigned to that cluster. This process is repeated.
  * */

  /******************************************************************************************************************
    * *********************************** 13.4 Network intrusion ****************************************************
    * **************************************************************************************************************/

  /* So-called cyberattacks are increasingly visible in the news. Some attacks attempt to flood a computer with
  * network traffic to crowd out legitimate traffic. But in other cases, attacks attempt to exploit flaws in
  * networking software to gain unauthorized access to a computer. While it’s quite obvious when a computer
  * is being bombarded with traffic, detecting an exploit can be like searching for a needle in an incredibly
  * large haystack of network requests.
  *
  * Some exploit behaviors follow known patterns. For example, accessing every port on a machine in rapid succession
  * is not something any normal software program should ever need to do. However, it is a typical first step for an
  * attacker looking for services running on the computer that may be exploitable. If you were to count the number
  * of distinct ports accessed by a remote host in a short time, you would have a feature that probably predicts
  * a port-scanning attack quite well. A handful is probably normal; hundreds indicates an attack. The same goes for
  * detecting other types of attacks from other features of network connections—number of bytes sent and received,
  * TCP errors, and so forth. But what about those unknown unknowns? The biggest threat may be the one that has
  * never yet been detected and classified. Part of detecting potential network intrusions is detecting anomalies.
  * These are connections that aren’t known to be attacks but do not resemble connections that have been observed
  * in the past.
  *
  * Here, unsupervised learning techniques like K-means can be used to detect anomalous network connections.
  * K-means can cluster connections based on statistics about each of them. The resulting clusters themselves
  * aren’t interesting per se, but they collectively define types of connections that are like past connections.
  * Anything not close to a cluster could be anomalous. Clusters are interesting insofar as they define regions
  * of normal connections; everything else outside is unusual and potentially anomalous.
  * */

  /****************************************************************************************************************
    * ****************************************** 13.5 Data Set ************************************************
    * *************************************************************************************************************/

  /* The data set which we use in this Lesson is from KDD Cup (http://kdd.ics.uci.edu/databases/kddcup99/kddcup99.html)
  *
  * Don’t use this data set to build a real network intrusion system! The data did not necessarily reflect real
  * network traffic at the time—even if it did, it reflects traffic patterns from 17 years ago.
  *
  * Fortunately, the organizers had already processed raw network packet data into summary information about
  * individual network connections. The data set is about 708 MB in size and contains about 4.9 million connections.
  * This is large, if not massive,and is certainly sufficient for our purposes here. For each connection, the data set
  * contains information like the number of bytes sent, login attempts, TCP errors, and so on. Each connection is
  * one line of CSV-formatted data set, containing 38 features, like this:
  * 0,tcp,http,SF,215,45076,0,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,1,1,0.00,0.00,0.00,0.00,1.00,0.00,0.00,0,0,0.00,
  * 0.00,0.00,0.00,0.00,0.00,0.00,0.00,normal.
  *
  * The above line represents a TCP connection to an HTTP service—215 bytes were sent and 45,706 bytes were received.
  * The user was logged in, and so on. Many features are counts, like num_file_creations in the 17th column
  *
  * Many features take on the value 0 or 1, indicating the presence or absence of a behavior, like su_attempted in
  * the 15th column. They look like the one-hot encoded categorical features, but are not grouped and related in
  * the same way.
  *
  * Each is like a yes/no feature (binary categorical), and is therefore arguably a categorical feature. It is not
  * always valid to translate categorical features as numbers and treat them as if they had an ordering. However,
  * in the special case of a binary categorical feature, in most machine learning algorithms, mapping these to a
  * numeric feature taking on values 0 and 1 will work well.
  *
  * The rest are ratios like dst_host_srv_rerror_rate in the next-to-last column, and take on values from 0.0 to 1.0,
  * inclusive. Interestingly, a label is given in the last field. Most connections are labeled normal., but some have
  * been identified as examples of various types of network attacks. These would be useful in learning to distinguish
  * a known attack from a normal connection, but the problem here is anomaly detection and finding potentially new
  * and unknown attacks. This label will be mostly set aside for our purposes.*/

  def main(args:Array[String]):Unit={
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val spark=SparkSession.builder().appName("Lesson13_Anomaly_Detection").master("local[2]").getOrCreate()
    import spark.implicits._

    val filePath="/DATA/data_set/spark/basics/Lesson13_Anomaly_Detection/kddcup.data"

    val rawWithoutHeader=spark.read.option("inferSchema","true").option("header","false").csv(filePath)

    val columnName=Seq("duration", "protocol_type", "service", "flag",
      "src_bytes", "dst_bytes", "land", "wrong_fragment", "urgent",
      "hot", "num_failed_logins", "logged_in", "num_compromised",
      "root_shell", "su_attempted", "num_root", "num_file_creations",
      "num_shells", "num_access_files", "num_outbound_cmds",
      "is_host_login", "is_guest_login", "count", "srv_count",
      "serror_rate", "srv_serror_rate", "rerror_rate", "srv_rerror_rate",
      "same_srv_rate", "diff_srv_rate", "srv_diff_host_rate",
      "dst_host_count", "dst_host_srv_count",
      "dst_host_same_srv_rate", "dst_host_diff_srv_rate",
      "dst_host_same_src_port_rate", "dst_host_srv_diff_host_rate",
      "dst_host_serror_rate", "dst_host_srv_serror_rate",
      "dst_host_rerror_rate", "dst_host_srv_rerror_rate",
      "label")
    val data = rawWithoutHeader.toDF(columnName:_*)
   // val colNum=data.columns.length
   // val rowNum=data.count()
   // println(s"Data has $colNum columns, $rowNum rows")
   // data.show(1)
    //data.printSchema()

    data.cache()

    /**************************************************************************************************************
      * ********************************************* 13.6 First take on Clustering *******************************
      * ************************************************************************************************************/

   // FirstTakeOnClustering(data)

    /************************************************************************************************************
      * ************************************************13.7 Choosing K ******************************************
      * ************************************************************************************************************/

    /* We know we have at least 23 distinct patterns in the data, so the k value could be at least 23, or even more.
    * We will try many k value to find the best k value. But what is the "best" K value?
    *
    * A clustering could be considered good if each data point were near its closest centroid, where “near” is defined
    * by the Euclidean distance. This is a simple, common way to evaluate the quality of a clustering, by the mean
    * of these distances over all points, or sometimes, the mean of the distances squared. In fact, KMeansModel offers
    * a computeCost method that computes the sum of squared distances and can easily be used to compute the mean
    * squared distance.
    *
    * Unfortunately, there is no simple Evaluator implementation to compute this measure, not like those available
    * to compute multiclass classification metrics. It’s simple enough to manually evaluate the clustering cost for
    * several values of k. Note that the following code could take 10 minutes or more to run.
    *
    * */

    //Calculate mean distance for each K
    //(20 to 100 by 20).map(k=> (k,ClusteringScoreForDifferentK(data,k))).foreach(println)

    /* We have tested for k=20,40,60,80,100.
    * (20,6.649218115128446E7)
    * (40,2.5031424366033625E7)
    * (60,1.027261913057096E7)
    * (80,1.2514131711109027E7)
    * (100,7235531.565096531)
    *
    * The printed result shows that the score decreases as k increases. Note that scores are shown in scientific
    * notation; the first value is over 10 pussiance 7, not just a bit over 6.
    *
    * Note the output of each execution is different, Because the clustering depends on a randomly chosen initial
    * set of centroids.
    *
    * However, this much is obvious, As more clusters are added, it should be possible to put data points closer to the
    * nearest centroid. If we go extreme, k value = number of data points, the average distance will be 0. Each data
    * point has it's own cluster.
    *
    * */

    /**************************************************************************************************************
      * ********************************************** 13.8 Choosing the starting point **************************
      * *********************************************************************************************************/

    /* In the previous output, we could notice the distance for k=80 is higher than for k=60. This shouldn't happen
    * because higher k always permits at least as good a clustering as a lower k. The problem is that K-means is not
    * necessarily able to find the optimal clustering for a given k. Its iterative process can converge from a random
    * starting point to a local mimimum, which may be good but is not optimal.
    *
    * This is still true even when more intelligent methods are used to choose initial centroids. K-means++ and K-means||
    * are variants of selection algorithms that are more likely to choose diverse, separated centroids and lead more
    * reliably to a good clustering. Spark MLlib implements K-means||. But all still have an element of randomness
    * in selection of starting point and can't guarantee an optimal clustering.
    *
    * We can improve it by running the iteration longer. The algorithm has a threshold via setTol() that controls the
    * minimum amount of cluster centroid movement considered significant. Lower values means algorithm will let the
    * centroids continue to move longer. Increasing the maximum number of iterations with setMaxIter() also prevents
    * it from potentially stopping too early at the cost of possibly more computation*/

// calculate mean distance for each k with better choosen starting point
  //  (20 to 100 by 20).map(k=> (k,ImproveStartingPointClusteringScore(data,k))).foreach(println)

    /* (20,5.8227534372047536E7)
     * (40,3.795519679283671E7)
     * (60,2.0655360813366067E7)
     * (80,1.1507239713147238E7)
     * (100,9888290.180070076)
     *
     * The output score is much better, as k value increase, mean distance decrease */

    /**************************************************************************************************************
      * ********************************************** 13.9 Feature Normalization ********************************
      * *********************************************************************************************************/

    /* As features value range can be small(0,..,100) and big (0,..,1000000000000000), this makes the data viz difficult
    * on a 3D projection.
    *
    * We can normalize each feature by converting it to a standard score. This means subtracting the mean of the
    * feature's values from each value, and dividing by the standard deviation, as shown in the standard score
    * equation: normalized_i=feature_i-m1/d1
    *
    * In fact, subtracting means has no effect on the clustering because the subtraction effectively shifts all the
    * data points by the same amount in the same directions. This does not affect interpoint Euclidean distances.
    *
    * ML and MLlib both provide StandardScaler, a component that can perform this kind of standardizaiton and be easily added to
     * the pipeline.*/

    // calculate mean distance for each k with better choosen starting point and feature normalization
   // (20 to 100 by 20).map(k=> (k,ClusteringScoreWithFeatureNormalization(data,k))).foreach(println)

    /*
    *  (20,7.412644144009894)
    * (40,2.5915895471974357)
    * (60,1.153548902077912)
    * (80,0.818219598187268)
    * (100,0.6208390857775707)
    * The feature Normalization has helped put dimensions on more equal footing, and the absolute distances between
    * points (and thus the cost) is much smaller in abolute terms. However, there isn't yet an obvious value of k
    * beyond which increasing it doess little to improve the cost.*/

    /**************************************************************************************************************
      * ********************************************** 13.10 Categorical Variables ********************************
      * *********************************************************************************************************/

    /* Normalization was a valuable step forward, but more can be done to improve the clustering. In particular,
    * several features have been left out entirely because they aren't numeric.
    *
    * To use the categorical features, we could use the one hot encoding to transform them into several binary numeric
    * features. For example, the second column contains the protocol type: tcp, udp, icmp. We could use three binary
    * features "is_TCP", "is_UDP", "is_ICMP" to represent this column.
    *
    * */

   // MyClusteringScoreWithCategoricalFeature(data,2)

   // (160 to 270 by 30).map(k=> (k,ClusteringScoreWithCategoricalFeature(data,k))).foreach(println)

    /* The following are the results of different k with one hot encoding of three Categorical feature
    * (160,1.982904044799585)
      (190,1.626247178667135)
      (220,1.1937676521871465)
      (250,0.9986357098095597)
    *
    *
      */

    /**************************************************************************************************************
      * ********************************************** 13.11 Using labels with Entropy ***************************
      * *********************************************************************************************************/

   /* In previous examples, we use existing labels to check the quality of our clustering and choosing k. A good
    * clustering, it seems, should agree with these human-applied labels. It should put together points that share a
    * label frequently and not lump together points of many different labels. It should produce clusters with relatively
    * homogeneous labels.
    *
    * In lesson 12, we have metrics for homogeneity: Gini impurity and entropy. These are functions of the proportions
    * of labels in each cluster, and produce a number that is low when the proportions are skewed toward few, or one,
    * label. The function entropy will be used here for illustration. */

    /* A good clustering would have clusters whose collections of labels are homogeneous and so have low entropy. A
     * weighted average of entropy can therefore be used as a cluster score. Generally, entropy refers to disorder
     * or uncertainty. For more information about Entropy, see https://en.wikipedia.org/wiki/Entropy_(information_theory)
     * */

   // (60 to 270 by 30).map(k => (k, ClusteringScoreWithLabelEntropy(data, k))).foreach(println)

    /*
     * (60,0.03475331900669869)
     * (90,0.051512668026335535)
     * (120,0.02020028911919293)
     * (150,0.019962563512905682)
     * (180,0.01110240886325257)
     * (210,0.01259738444250231)
     *
     * Here again, results suggest k=180 is a reasonable choice because its score is actually lower than 150 and 210
     * */

    /* Now with k=180, we can print the labels for each cluster to get some sense of the resulting clustering.
    * Clusters do seem to be dominated by one type of attack each, and contain only a few types.
    */

  /*  val pipelineModel=FullKMeansPipeLine(data,180)
    val countByClusterLabel=pipelineModel.transform(data)
        .select("cluster","label")
      .groupBy("cluster","label").count()
      .orderBy("cluster","label")
    countByClusterLabel.show() */

    /**************************************************************************************************************
      * ********************************************** 13.12 Anomaly Detection ***************************
      * *********************************************************************************************************/

    /* Now we can make an actual anomaly detector. Anomaly detection amounts to measuring a new data point's distance
    * to its nearest centroid. If this distance exceeds some threshold, it it anomalous. This threshold might be
    * chosen to be the distance of, say, the 100th-farthest data point from among know data
    * */

   // AnomalyDetection(data)

    /* The output is [9,tcp,telnet,SF,307,2374,0,0,1,0,0,1,0,1,0,1,3,1,0,0,0,0,1,1,0.0,0.0,0.0,0.0,1.0,0.0,0.0,69,
     * 4,0.03,0.04,0.01,0.75,0.0,0.0,0.0,0.0,normal.]
     *
     * Althought, the label says normal, but we could notice that this connections connect to 69 different hosts.
     *
     *
     **/

    /**************************************************************************************************************
      * ********************************************** 13.13 Future works *****************************************
      * *********************************************************************************************************/

    /* 1. The kMean model which we use is only a simplistic one. For example, Euclidean distance is used in this
    * example. Because it is the only distance function supported by ML lib at this time. In the future, we may use
    * distance functions that can better account for the distributions of and correlations between features, such as
     * Mahalanobis distance (https://en.wikipedia.org/wiki/Mahalanobis_distance)
     *
     * 2. There are also sophisticated cluster-quality evaluation metrics that could be applied (even without labels)
     * to pick k, such as the Silhouette coefficient (https://en.wikipedia.org/wiki/Silhouette_(clustering)). These
     * tend to evaluate not just closeness of points within one cluster, but closeness of points to other clusters.
     *
     * Finally, different models could be applied instead of simple K-means clustering; for example,
     * - Gaussian mixture model (https://en.wikipedia.org/wiki/Mixture_model#Gaussian_mixture_model)
     * - DBSCAN https://en.wikipedia.org/wiki/DBSCAN
     * These two could capture more subtle relationships between data points and the cluster centers. Spark Ml lib
     * aleady implements Gaussian mixture models.
     *
     * 3. The code in this Lesson cloud be used within Spark Streaming to score*/
  }

  /*************************************************13.6 First model **************************************/
  def FirstTakeOnClustering(data:DataFrame):Unit={
    /* As we noticed before, we have a label column. Let's see how many different labels we have in this data set*/
   // data.select("label").groupBy("label").count().orderBy(desc("count")).show(25)

    /* You will find the following output, There are 23 distinct labels, the most frequent are smurf. and neptune.
    *attacks

|           label|  count|
+----------------+-------+
|          smurf.|2807886|
|        neptune.|1072017|
|         normal.| 972781|
|          satan.|  15892|


*/



    /* We have noticed we have non-numeric columns in our dataset, for example second column is a categorical feature
    * which could have values such as tcp, udp, icmp.
    *
    * But K-mean clustering model only accept numeric features. For our first model, we will just ignore all non numeric
     * features*/

    val nonNumericFeature=Array("protocol_type","service","flag")

    val DataWithOnlyNumericFeature=data.drop(nonNumericFeature:_*)
    val numericFeatureCol=DataWithOnlyNumericFeature.columns.filter(_ !="label")
    val assembler = new VectorAssembler().setInputCols(numericFeatureCol).setOutputCol("featureVector")

    val kmeans = new KMeans().setPredictionCol("cluster").setFeaturesCol("featureVector")

    val pipeline = new Pipeline().setStages(Array(assembler,kmeans))
    val pipelineModel = pipeline.fit(DataWithOnlyNumericFeature)

    val kmeansModel=pipelineModel.stages.last.asInstanceOf[KMeansModel]
    //kmeansModel.clusterCenters.foreach(println)

    /* The output of kmeansModel clusterCenters is two vectors of coordinates which describe the center of two cluster
    * (aka. centroid). As there are only two vectors, we cloud say K-means was fitting k=2 clusters to the data. We
    * know that we have at least 25 different groups, so k=2 will never give us a accurate model which can do the
    * clustering.
    *
    * The following code will tell us how the output clusters matched the labels which we know */

    val DataWith2Cluster=pipelineModel.transform(DataWithOnlyNumericFeature)

    DataWith2Cluster.select("cluster","label").groupBy("cluster","label").count().orderBy(col("cluster"),desc("count")).show(25)

    /* We could notice only label portsweep is in cluster 1 and all other labels are in cluster 0. */



  }
/******************************************13.7 Choosing K ********************************************/
def ClusteringScoreForDifferentK(data:DataFrame,k:Int):Double={
  val nonNumericFeature=Array("protocol_type","service","flag")

  val DataWithOnlyNumericFeature=data.drop(nonNumericFeature:_*)
  val numericFeatureCol=DataWithOnlyNumericFeature.columns.filter(_ !="label")
  val assembler = new VectorAssembler().setInputCols(numericFeatureCol).setOutputCol("featureVector")
  val kmeans=new KMeans().setSeed(Random.nextLong())
    .setK(k)
    .setPredictionCol("cluster")
    .setFeaturesCol("featureVector")

  val pipeline=new Pipeline().setStages(Array(assembler,kmeans))

  val kmeansModel=pipeline.fit(data).stages.last.asInstanceOf[KMeansModel]
  //compute mean from total squared distance ("cost")
  kmeansModel.computeCost(assembler.transform(data))/data.count()
}

  /**********************************13.8 Choosing the starting point ************************************/

  def ImproveStartingPointClusteringScore(data:DataFrame,k:Int):Double={
    val nonNumericFeature=Array("protocol_type","service","flag")

    val DataWithOnlyNumericFeature=data.drop(nonNumericFeature:_*)
    val numericFeatureCol=DataWithOnlyNumericFeature.columns.filter(_ !="label")
    val assembler = new VectorAssembler().setInputCols(numericFeatureCol).setOutputCol("featureVector")
    val kmeans=new KMeans().setSeed(Random.nextLong())
      .setK(k)
      .setPredictionCol("cluster")
      .setFeaturesCol("featureVector")
      //increase from the default value is 20
      .setMaxIter(40)
      //decrease from the default value 1.0e-4
      .setTol(1.0e-5)

    val pipeline=new Pipeline().setStages(Array(assembler,kmeans))

    val kmeansModel=pipeline.fit(data).stages.last.asInstanceOf[KMeansModel]
    //compute mean from total squared distance ("cost")
    //computeCost method evaluate clustering by computing Within Set(cluster) Sum of Squared Errors
    /* */
    kmeansModel.computeCost(assembler.transform(data))/data.count()
  }

  /*************************************13.9 Feature Normalization ******************************************/
  def ClusteringScoreWithFeatureNormalization(data:DataFrame,k:Int):Double={
    val nonNumericFeature=Array("protocol_type","service","flag")

    val DataWithOnlyNumericFeature=data.drop(nonNumericFeature:_*)
    val numericFeatureCol=DataWithOnlyNumericFeature.columns.filter(_ !="label")
    val assembler = new VectorAssembler().setInputCols(numericFeatureCol).setOutputCol("featureVector")
    //Feature normalization
    val scaler = new StandardScaler()
      .setInputCol("featureVector")
      .setOutputCol("scaledFeatureVector")
      .setWithStd(true)
      .setWithMean(false)

    val kmeans=new KMeans()
      .setSeed(Random.nextLong())
      .setK(k)
      .setPredictionCol("cluster")
      .setFeaturesCol("scaledFeatureVector")
      .setMaxIter(40)
      .setTol(1.0e-5)

    val pipeline=new Pipeline().setStages(Array(assembler,scaler,kmeans))
    val pipelineModel=pipeline.fit(data)
    val kMeansModel=pipelineModel.stages.last.asInstanceOf[KMeansModel]
    kMeansModel.computeCost(pipelineModel.transform(data))/data.count()

  }

  /****************************** 13.10 One hot code Categorical Feature *****************************/

  def OneHotPipeLine(inputCol:String):(Pipeline,String)={
    /*
    * The StringIndexer will read all possible value of a categorical value in a dataset, and encoded with numeric
    * values, in our example, we have the following result for protocol_type
    *
    * +-------------+---------------------+
    * |protocol_type|protocol_type_indexed|
    * +-------------+---------------------+
    * |          udp|                  2.0|
    * |          tcp|                  1.0|
    * |         icmp|                  0.0|
    * +-------------+---------------------+
    * */
    val indexer = new StringIndexer()
      .setInputCol(inputCol)
      .setOutputCol(inputCol+"_indexed")

    val encoder = new OneHotEncoderEstimator()
      .setInputCols(Array(inputCol+"_indexed"))
      .setOutputCols(Array(inputCol+"_vec"))

    val pipeline = new Pipeline().setStages(Array(indexer,encoder))
   // return the pipeline and name of the output column
    (pipeline,inputCol+"_vec")
  }

  /* Break down the one hot encoder pipeline into steps with out put*/
  def MyClusteringScoreWithCategoricalFeature(data:DataFrame,k:Int):Double={
    val categoricalFeatures=Array("protocol_type","service","flag")

    val feature="protocol_type"

    val indexer=new StringIndexer().setInputCol(feature).setOutputCol(feature+"_indexed")

    val indexModel=indexer.fit(data)

    val indexData=indexModel.transform(data)

   // indexData.select(feature,feature+"_indexed").distinct().show(5)

    // OneHotEncoder is deprecated since spark 2.3, We can use OneHotEncoderEstimator to replace it.

    /*val encoder = new OneHotEncoder()
      .setInputCol(feature+"_indexed")
      .setOutputCol(feature+"_vec")

    val encodedData=encoder.transform(indexData)*/

    val encoder = new OneHotEncoderEstimator()
      .setInputCols(Array(feature+"_indexed"))
      .setOutputCols(Array(feature+"_vec"))

    val encoderModel = encoder.fit(indexData)

    val encodedData = encoderModel.transform(indexData)

    encodedData.select(feature,feature+"_indexed",feature+"_vec").distinct().show(5)

    /* The output of one hot encoder in spark is not binary numeric columns. The output is a description of a vector
    * The description is an Array of three elements, first element is the length of the vector, second element is the
    * position of value, the third element is the value
    *
    *
    * +-------------+---------------------+-----------------+
    * |protocol_type|protocol_type_indexed|protocol_type_vec|
    * +-------------+---------------------+-----------------+
    * |          tcp|                  1.0|    (2,[1],[1.0])|
    * |          udp|                  2.0|        (2,[],[])|
    * |         icmp|                  0.0|    (2,[0],[1.0])|
    * +-------------+---------------------+-----------------+
    *
    * The above output is the onehot encoding of feature protocol_type,
    * icmp -> (stringIndexer) 0 -> 10 (vec encoding) -> (2,[0],[1.0]). The vector has length 2, and position 0
    * has value 1.0. Note that the position of 10 starts from left to right. So value 1 position is 0 not 1.
    * */
    /*for(features <- categoricalFeatures){}*/

    return 0.0
  }

  def ClusteringScoreWithCategoricalFeature(data:DataFrame,k:Int):Double={
    val (protoTypeEncoder, protoTypeVecCol) = OneHotPipeLine("protocol_type")
    val (serviceEncoder, serviceVecCol) = OneHotPipeLine("service")
    val (flagEncoder, flagVecCol) = OneHotPipeLine("flag")

    // Original columns, without label / string columns, but with new vector encoded cols
    // We can use -- and ++ to add or remove element from a Set in scala.
    val assembleCols = Set(data.columns: _*)--
      Seq("label", "protocol_type", "service", "flag")++
      Seq(protoTypeVecCol, serviceVecCol, flagVecCol)

    val assembler = new VectorAssembler().
      setInputCols(assembleCols.toArray).
      setOutputCol("featureVector")

    val scaler = new StandardScaler()
      .setInputCol("featureVector")
      .setOutputCol("scaledFeatureVector")
      .setWithStd(true)
      .setWithMean(false)

    val kmeans = new KMeans().
      setSeed(Random.nextLong()).
      setK(k).
      setPredictionCol("cluster").
      setFeaturesCol("scaledFeatureVector").
      setMaxIter(40).
      setTol(1.0e-5)

    val pipeline = new Pipeline().setStages(
      Array(protoTypeEncoder, serviceEncoder, flagEncoder, assembler, scaler, kmeans))
    val pipelineModel = pipeline.fit(data)

    val kmeansModel = pipelineModel.stages.last.asInstanceOf[KMeansModel]
    kmeansModel.computeCost(pipelineModel.transform(data)) / data.count()
  }

  /************************************** 13.11 Using labels with entropy ****************************/

  def entropy(counts: Iterable[Int]):Double={
    // get all positive values in the counts collection
    val values=counts.filter(_ >0)
    // cast all values to double and do sum
    val n = values.map(_.toDouble).sum
    val entropy=values.map{v=>
      //calculate p first
      val p=v / n
      //
      -p * math.log(p)
    }.sum

    /* We can use the following code, if you don't want to define a local variable

    values.map { v =>
      -(v / n) * math.log(v / n)
    }.sum
    */

    return entropy
  }
  def FullKMeansPipeLine(data:DataFrame,k:Int):PipelineModel={
    // transform the string categorical feature column into one hot encoding column
    val (protoTypeEncoder, protoTypeVecCol) = OneHotPipeLine("protocol_type")
    val (serviceEncoder, serviceVecCol) = OneHotPipeLine("service")
    val (flagEncoder, flagVecCol) = OneHotPipeLine("flag")

    // Start with the original columns, remove label and string columns, and adding new vector one hot encoded cols
    val assembleCols = Set(data.columns: _*) --
      Seq("label", "protocol_type", "service", "flag") ++
      Seq(protoTypeVecCol, serviceVecCol, flagVecCol)

    // Take all feature column and transform them into a Vector
    val assembler = new VectorAssembler().
      setInputCols(assembleCols.toArray).
      setOutputCol("featureVector")

    // normalize the feature
    val scaler = new StandardScaler()
      .setInputCol("featureVector")
      .setOutputCol("scaledFeatureVector")
      .setWithStd(true)
      .setWithMean(false)

    // build the kmeans model
    val kmeans = new KMeans().
      setSeed(Random.nextLong()).
      setK(k).
      setPredictionCol("cluster").
      setFeaturesCol("scaledFeatureVector").
      setMaxIter(40).
      setTol(1.0e-5)

    // build the pipeline with above steps
    val pipeline = new Pipeline().setStages(
      Array(protoTypeEncoder, serviceEncoder, flagEncoder, assembler, scaler, kmeans))

    // train the model
    pipeline.fit(data)
  }

  def ClusteringScoreWithLabelEntropy(data:DataFrame,k:Int):Double={
    val spark=data.sparkSession
    import spark.implicits._
    // get the trained kmean model
    val pipelineModel=FullKMeansPipeLine(data,k)

    //predict cluster for each label with the trained model
    val clusterLabel=pipelineModel.transform(data).select("cluster","label").as[(Int,String)]

    //calculate the cluster entropy
    val weightedClusterEntropy = clusterLabel
      // Extract collections of labels, per cluster
      .groupByKey{case(cluster,_)=>cluster}
      .mapGroups{case(_,clusterLabels)=>
        val labels = clusterLabels.map { case (_, label) => label }.toSeq
        // Count labels in collections
        val labelCounts = labels.groupBy(identity).values.map(_.size)
        labels.size * entropy(labelCounts)
      }.collect()
    // Average entropy weighted by cluster size
    weightedClusterEntropy.sum / data.count()
  }

  def AnomalyDetection(data:DataFrame):Unit={
    val spark=data.sparkSession
    import spark.implicits._
    val pipelineModel=FullKMeansPipeLine(data,180)
    // get the trained model
    val kMeansModel=pipelineModel.stages.last.asInstanceOf[KMeansModel]
   // get the centroids of all clusters
    val centroids=kMeansModel.clusterCenters

    // predict clusters of the data set
    val clusteredData=pipelineModel.transform(data)

    val threshold = clusteredData.select("cluster","scaledFeatureVector").as[(Int,Vector)]
      .map{case(cluster,vec)=>Vectors.sqdist(centroids(cluster),vec)}
      .orderBy($"value".desc)
      //single output implicitly named "value"
      .take(100).last


    val originalCols = data.columns
    // filter all rows which sqaure distance is greater than the threshold
    val anomalies = clusteredData.filter { row =>
      val cluster = row.getAs[Int]("cluster")
      val vec = row.getAs[Vector]("scaledFeatureVector")
      Vectors.sqdist(centroids(cluster), vec) >= threshold
    }.select(originalCols.head, originalCols.tail:_*)

    // we use first to make the read easy, show works too
    println(anomalies.first())
  }
}
