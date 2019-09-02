package org.pengfei.Lesson01_RDD

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Lesson01_RDDWithNumTypes {

  /* RDDs containing data elements of type Integer, Long, Float, or Double support a few additional actions that
   * are useful for statistical analysis.*/

  def main(args:Array[String])={
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark=SparkSession.builder().master("local[2]").appName("Lesson1_RDDWithNumTypes").getOrCreate()
    val sc = spark.sparkContext

    /************************************************************************************************
      * *********************************RDD with num type special actions **************************
      * **********************************************************************************************/
    val nums=sc.parallelize(List(2,5,3,1,9,6))

    /************************************** mean *************************************************/
    /* The mean method returns the average of the elements in the source RDD */
    val mean= nums.mean()
    println(s"Mean of nums : ${mean}")

    /* *************************************** stdev ********************************************/
    /* The stdev method returns the standard deviation of the elements in the source RDD.
    *
    * In statistics, the standard deviation (SD, also represented by the Greek letter sigma σ or the Latin letter s)
    * is a measure that is used to quantify the amount of variation or dispersion of a set of data values.
    * A low standard deviation indicates that the data points tend to be close to the mean
    * (also called the expected value) of the set, while a high standard deviation indicates that the
    * data points are spread out over a wider range of values.*/
    val stdev = nums.stdev()
    println(s"Stdev of nums : ${stdev}")

    /********************************* sum *******************************************************/
  /*The sum method returns the sum of the elements in the source RDD.*/
    val sum=nums.sum()
    println(s"sum of nums: ${sum}")

    /********************************** Variance ***************************************************/
  /* The variance method returns the variance of the elements in the source RDD.
   * In probability theory and statistics, variance is the expectation of the squared deviation of
   * a random variable from its mean. Informally, it measures how far a set of (random) numbers are spread out
   * from their average value. Variance has a central role in statistics, where some ideas that use it include
   * descriptive statistics, statistical inference, hypothesis testing, goodness of fit, and Monte Carlo sampling.
   * Variance is an important tool in the sciences, where statistical analysis of data is common. The variance is
   * the square of the standard deviation, the second central moment of a distribution, and the covariance of the
   * random variable with itself, and it is often represented by s2 or Var(X).*/
    val variance=nums.variance()
    println(s" variance of nums: ${variance}")
  }

}
