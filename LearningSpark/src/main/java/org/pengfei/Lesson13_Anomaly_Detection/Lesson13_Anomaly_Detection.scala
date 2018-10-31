package org.pengfei.Lesson13_Anomaly_Detection

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

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
    val colNum=data.columns.length
    val rowNum=data.count()
   // println(s"Data has $colNum columns, $rowNum rows")
   // data.show(1)

FirstTakeOnClustering(data)
    /**************************************************************************************************************
      * ********************************************* 13.6 First take on Clustering *******************************
      * ************************************************************************************************************/


  }

  def FirstTakeOnClustering(data:DataFrame):Unit={
    /* As we noticed before, we have a label column. Let's see how many different labels we have in this data set*/
    data.select("label").groupBy("label").count().orderBy(col("count").desc).show(25)

  }

}
