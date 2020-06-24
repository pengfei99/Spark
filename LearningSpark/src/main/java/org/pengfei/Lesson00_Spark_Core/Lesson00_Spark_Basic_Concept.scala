package org.pengfei.Lesson00_Spark_Core

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Lesson00_Spark_Basic_Concept {

  /*********************************************************************************************
    * *****************************0. Spark introduction ***************************************
    * ******************************************************************************************/

  /* Spark is an in-memory cluster computing framework for processing and analyzing large amounts of
  * data, It provides a simple programming interface, which enables an applications developer to easily
  * use the cpu, memory, and storage resources across a cluster of servers for processing large datasets */


  /*Key components of a spark cluster
  * 1.Master Node
  * 1.1 Driver Program : A driver program is an spark application that uses Spark as a library. It provides the data
  *                      processing code that Spark executes on the worker nodes. A driver program can launch one or
  *                      more jobs on a Spark cluster.
  * 1.2 Cluster Manager :(standalone(spark default resource manager), yarn, mesos, ec2, Kubernetes), cluster manager
  *                      (resource manager)
  *                      can acquire cluster resource for executing a job. It provides low-level scheduling of cluster
  *                      resources across applications. It enables multiple applications to share cluster resources
  *                      and run on the same worker nodes.
  *
  * 2. Worker Node : worker node provides CPU, memory, and storage to a spark application.
  * 2.1 Executor : spark executors runs on worker node as distributed process of a Spark application(aka. driver
  *                program). An executor is a JVM process that Spark creates on each worker for an application.
  *                It executes application code concurrently in multiple threads. It can also cache data in memory
  *                or disk. An executor has the same lifespan as the application for which it is created. When a Spark
  *                application terminates, all executors created for it also terminate.
  *
  * 2.2 Tasks : A task is the smallest unit of work that Spark sends to an executor. It is executed by a thread in an
  *             executor on a worker node. Each task performs some computations to either return a result to a driver
  *             program or partition its output for shuffle. Spark creates a task per data partition. An executor runs
  *             one or more tasks concurrently. The amount of parallelism is determined by the number of partitions.
  *             More partitions mean more tasks processing data in parallel.
  *
  * 2.3 Executor number on a worker : If you specify the amount of executors when invoking spark-submit you should get
  *                       the amount you ask for –num-executors X If you do not specify then by default Spark should
  *                       use dynamic allocation which will start more executors if needed. In this case you can
  *                       configure the behaviour, e.g. max number of executors,
  *                       see http://spark.apache.org/docs/latest/configuration.html#dynamic-allocation
  * */

  /*
  * Other important terminology
  * Shuffle: A shuffle redistributes data among a cluster of nodes. It is an expensive operation because it
  *          involves moving data across a network. Note that a shuffle does not randomly redistribute data;
  *          it groups data elements into buckets based on some criteria. Each bucket forms a new partition.
  * Job: A job is a set of computations that Spark performs to return results to a driver program. Essentially,
  *      it is an execution of a data processing algorithm on a Spark cluster. An application can launch multiple jobs.
  * Stage: A stage is a collection of tasks. Spark splits a job into a DAG of stages. A stage may depend on another
  *       stage. For example, a job may be split into two stages, stage 0 and stage 1, where stage 1 cannot begin
  *       until stage 0 is completed. Spark groups tasks into stages using shuffle boundaries. Tasks that do not
  *       require a shuffle are grouped into the same stage. A task that requires its input data to be shuffled begins
  *       a new stage.
*/

  /* For more details of spark introduction, please visit my wiki page
  employes:pengfei.liu:big_data:spark:l01_spark_introduction*/
  def main(args:Array[String]): Unit ={
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = SparkSession.builder().
      master("local[2]"). //spark://10.70.3.48:7077 remote
      appName("LessonO_Spark_Core").
      getOrCreate()
    // import sparkSession.implicits._ for all schema conversion magic.

    //you can get sparkContext with sparkSession
    val sc=spark.sparkContext
    val sqlc=spark.sqlContext

    //Create a rdd with a list
    val rddExample=sc.parallelize(List("I love meat","apple","orange","it's ok"))
    println(s"RDD contains ${rddExample.count()} element" )
    println(s"RDD content values : ${rddExample.collect().toList.toString()}")

    //We will see how to play with rdd in the next lesson
  }
}
