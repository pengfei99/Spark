package org.pengfei.Lesson05_Spark_ML

import org.apache.spark.sql.SparkSession

object Lesson05_2_2_1_Clustering_Algo {

  /******************************************************************************************************************
    * *****************************************5.2.2.1 Clustering Algorithms *****************************************
    * **************************************************************************************************************/
  /* Clustering is a Machine Learning technique that involves the grouping of data points. Given a set of data points,
   * we can use a clustering algorithm to classify each data point into a specific group. In theory, data points that
   * are in the same group should have similar properties and/or features, while data points in different groups should
   * have highly dissimilar properties and/or features. Clustering is a method of unsupervised learning and is a
   * common technique for statistical data analysis used in many fields.
   *
   * In Data Science, we can use clustering analysis to gain some valuable insights from our data by seeing what
   * groups the data points fall into when we apply a clustering algorithm. Here, we’re going to look at 5 popular
   * clustering algorithms that data scientists need to know and their pros and cons!
   *
   * The below 5 algo are from https://towardsdatascience.com/the-5-clustering-algorithms-data-scientists-need-to-know-a36d136ef68*/

  def main(args:Array[String])={

  }

  /***********************************************K-means/K-median algo******************************************/
  def KMeansExample(spark:SparkSession):Unit={
    /*
    * The k-means algorithm finds groupings or clusters in a dataset. It is an iterative algorithm that partitions
    * data into k mutually exclusive clusters, where k is a number specified by a user.
    *
    * The k-means algorithm uses a criterion known as within-cluster sum-of-squares for assigning
    * observations to clusters. It iteratively finds cluster centers and assign observations to clusters such that
    * within-cluster sum-of-squares is minimized.
    *
    * The number of clusters in which data should be segmented is specified as an argument to the k-means algorithm.
    * The k-mean algo has four principal steps
    * Step1 : we select a number of classes/groups to use and randomly initialize their respective center points.
    *         To figure out the number of classes to use, it’s good to take a quick look at the data and try to
    *         identify any distinct groupings. The center points are vectors of the same length as each data
    *         point vector and are the “X’s” in the graphic above.
    * Step2 : Each data point is classified by computing the distance between that point and each group center,
    *         and then classifying the point to be in the group whose center is closest to it.
    * Step3 : Based on these classified points, we recompute the group center by taking the mean of all the vectors
    *         in the group.
    * Step4 : Repeat these steps for a set number of iterations or until the group centers don’t change much between
    *         iterations. You can also opt to randomly initialize the group centers a few times, and then select the
    *         run that looks like it provided the best results.
    *
    * K-Means has the advantage that it’s pretty fast, as all we’re really doing is computing the distances between
    * points and group centers; very few computations! It thus has a linear complexity O(n).
    *
    * On the other hand, K-Means has a couple of disadvantages. Firstly, you have to select how many groups/classes
    * there are. This isn’t always trivial and ideally with a clustering algorithm we’d want it to figure those out
    * for us because the point of it is to gain some insight from the data. K-means also starts with a random choice
    * of cluster centers and therefore it may yield different clustering results on different runs of the algorithm.
    * Thus, the results may not be repeatable and lack consistency. Other cluster methods are more consistent.
    *
    * K-Medians is another clustering algorithm related to K-Means, except instead of recomputing the group center
    * points using the mean we use the median vector of the group. This method is less sensitive to outliers
    * (because of using the Median) but is much slower for larger datasets as sorting is required on each iteration
    * when computing the Median vector.
    *    */

    /**************************************************Mean-Shift Clustering*****************************************/
    def MeanShiftClusteringExample(spark:SparkSession):Unit={
      /* Mean shift clustering is a sliding-window-based algorithm that attempts to find dense areas of data points.
      * It is a centroid-based algorithm meaning that the goal is to locate the center points of each group/class,
      * which works by updating candidates for center points to be the mean of the points within the sliding-window.
      * These candidate windows are then filtered in a post-processing stage to eliminate near-duplicates, forming
      * the final set of center points and their corresponding groups. Check out the graphic below for an illustration.*/
    }
    /**/
  }
}
