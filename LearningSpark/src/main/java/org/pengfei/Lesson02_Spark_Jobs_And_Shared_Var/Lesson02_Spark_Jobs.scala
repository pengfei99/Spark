package org.pengfei.Lesson02_Spark_Jobs_And_Shared_Var

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Lesson02_Spark_Jobs {
def main(args:Array[String])={
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  def spark=SparkSession.builder().master("local[2]").appName("Lesson2_Spark_Jobs").getOrCreate()
  def sc=spark.sparkContext

  /********************************************************************************************************
    *****************************************2.1 Spark Jobs************************************************
    * *****************************************************************************************************/

  /* 2.1.1 Introduction
  * RDD operations, including transformation, action and caching methods form the basis of a Spark
  * application. Essentially, RDDs describe the Spark programming model. Now that we have covered the
  * programming model, we will discuss how it all comes together in a Spark application
  *
  * 2.1.2 What is a spark job?
  * A job is a set of computations that Spark performs to return the results of an action to a driver program.
  * An application can launch one or more jobs. It launches a job by calling an action method of an RDD. Thus,
  * an action method triggers a job. If an action is called for an RDD that is not cached or a descendant of a
  * cached RDD, a job starts with the reading of data from a storage system. However, if an action is called for
  * an RDD that is cached or a descendent of a cached RDD, a job begins from the point at which the RDD or its
  * ancestor RDD was cached. Next, Spark applies the transformations required to create the RDD whose action
  * method was called. Finally, it performs the computations specified by the action. A job is completed when a
  * result is returned to a driver program.
  *
  * 2.1.3 What is task stages in a spark job?
  * When an application calls an RDD action method, Spark creates a DAG of task stages. It groups tasks into stages
  * using shuffle boundaries. Tasks that do not require a shuffle are grouped into the same stage. A task that requires
  * its input data to be shuffled begins a new stage. A stage can have one or more tasks. Spark submits tasks to
  * the executors, which run the tasks in parallel. Tasks are scheduled on nodes based on data locality. If a node
  * fails while working on a task, Spark resubmits task to another node.
  *
  * 2.1.4 Shuffle operation
  * The shuffle operation is Spark’s mechanism for re-distributing data so that it’s grouped differently across
  * partitions. This typically involves copying data across executors and machines, making the shuffle a complex
  * and costly operation.
  *
  * The Shuffle is an expensive operation since it involves disk I/O, data serialization, and network I/O.
  * To organize data for the shuffle, Spark generates sets of tasks - map tasks to organize the data, and a set of
  * reduce tasks to aggregate it. This nomenclature comes from MapReduce and does not directly relate to Spark’s
  * map and reduce operations.
  *
  * Which RDD transformations and actions can cause shuffle operations?
  * This is hard to answer, because many transformations may or may not cause shuffle operations. It depends on many
  * factors.
  *
  * Below is a list which may cause shuffle operations.
  * -cogroup
  * -groupwith
  * -join
  * -leftOuterJoin
  * -rightOuterJoin
  * -groupByKey
  * -reduceByKey
  * -combineByKey
  * -sortByKey
  * -distinct
  * -intersection
  * -repartition : Very high chance to cause shuffle
  * -coalesce : Very high chance to cause shulle
  *
  * The best way to know is to check your spark job DAG, One stage means one shuffle.
  * You can also use toDebugString method, But beware, toDebugString returns "A description of this RDD and its
  * recursive dependencies for debugging." So it will include possible shuffles from prior transformations if they exist.
  * Check well the output of toDebugString method to make sure which transformation causes shuffle.
  *
  *
  * */

  val rdd=sc.parallelize(List("orange","apple","banana","kiwi"))
  println(s" distinct on rdd need shuffle or not: ${rdd.distinct().toDebugString}")

  /********************************************************************************************************
    *****************************************2.2 Shared Variables*********************************************
    * *****************************************************************************************************/

  /* Spark uses a shared-nothing architecture. Data is partitioned across a cluster of nodes and each node in
   * a cluster has its own CPU, memory, and storage resources. There is no global memory space that can be
   * shared by the tasks. The driver program and job tasks share data through messages.
   *
   * For example, if a function argument to an RDD operator references a variable in the driver program, Spark
   * sends a copy of that variable along with a task to the executors. Each task gets its own copy of the variable
   * and uses it as a read-only variable. Any update made to that variable by a task remains local. Changes are
   * not propagated back to the driver program. In addition, Spark ships that variable to a worker node at the
   * beginning of every stage.
   *
   * This default behavior can be inefficient for some applications. In one use case, the driver program
   * shares a large lookup table with the tasks in a job and the job involves several stages. By default, Spark
   * automatically sends the driver variables referenced by a task to each executor; however, it does this for each
   * stage. Thus, if the lookup table holds 100 MB data and the job involves ten stages, Spark will send the same
   * 100 MB data to each worker node ten times.
   *
   * Another use case involves the ability to update a global variable in each task running on different nodes.
   * By default, updates made to a variable by a task are not propagated back to the driver program.
   * */

  /********************************************2.2.1 Broadcast variables ******************************************/

  /* Broadcast variables enable a Spark application to optimize sharing of data between the driver program
   * and the tasks executing a job. Spark sends a broadcast variable to a worker node only once and caches it in
   * deserialized form as a read-only variable in executor memory. In addition, it uses a more efficient algorithm
   * to distribute broadcast variables.
   *
   * Note that a broadcast variable is useful if a job consists of multiple stages and tasks across stages
   * reference the same driver variable. It is also useful if you do not want the performance hit from having to
   * deserialize a variable before running each task. By default, Spark caches a shipped variable in the executor
   * memory in serialized form and deserializes it before running each task.
   *
   * The SparkContext class provides a method named broadcast for creating a broadcast variable. It takes
   * the variable to be broadcasted as an argument and returns an instance of the Broadcast class. A task must
   * use the value method of a Broadcast object to access a broadcasted variable.
   *
   * */

  case class Transaction(id:Long,cusId:Int,itemId:Int)
  case class TransactionDetail(id:Long,custName:String,itemName:String)

  val customerMap=Map(1->"Tom",2->"Bob")
  val itemMap=Map(1->"Car",2->"Phone")


  val transactions=sc.parallelize(List(Transaction(1,1,1),Transaction(2,1,2)))

  val bcCustomerMap=sc.broadcast(customerMap)
  val bcItemMap=sc.broadcast(itemMap)

  val transactionDetails=transactions.map{t:Transaction=>
                TransactionDetail(t.id,bcCustomerMap.value(t.cusId),bcItemMap.value(t.itemId))}

  //Without using broadcast variables, it works too, but it will take more times, because of deserailization
 /* val transactionDetails=transactions.map{t:Transaction=>
                TransactionDetail(t.id,customerMap.get(t.cusId).get,itemMap.get(t.itemId).get)}*/

  println(s"Transaction details value: ${transactionDetails.collect.mkString("||")}")

  /* The use of broadcast variables enabled us to implement an efficient join between the customer, item
   * and transaction dataset. We could have used the join operator from the RDD API, but that would shuffle
   * customer, item, and transaction data over the network. Using broadcast variables, we instructed Spark
   * to send customer and item data to each node only once and replaced an expensive join operation with a
   * simple map operation.*/

  /*********************************** Understanding closures ****************************************************/

  /*
  * Before we talk Accumulator, we need to understand the scope and life cycle of variables and methods when
  * executing spark job across a cluster. RDD operations that modify variables outside of their scope can be a frequent
  * source of confusion. In the example below we’ll look at code that uses foreach() to increment a counter, but
  * similar issues can occur for other operations as well.
  *
  *
  * var counter = 0
  * var rdd = sc.parallelize(data)
  * // Wrong: Don't do this!!
  * rdd.foreach(x => counter += x)
  * println("Counter value: " + counter)
  *
  * The above code may work in local mode, because the counter and the closure foreach are in the same JVM.
  * But in cluster mode, the rdd are split in the cluster, and the closure for each is executed in each executor.
  * As a result the counter in slave node executor are updated, but no in the spark driver. So the result will be
  * always 0. To solve this kind of problems, we need to use Accumulators.
  *
  * For more details, visite https://spark.apache.org/docs/2.2.0/rdd-programming-guide.html#accumulators
  * */

  /**************************** 1.6 Accumulators  ****************************************************************/

  /*
  * Accumulators are variables that are only “added” to through an associative and commutative operation and can
  * therefore be efficiently supported in parallel. They can be used to implement counters (as in MapReduce) or sums.
  * Spark natively supports accumulators of numeric types, and programmers can add support for new types.
  *
  * The SparkContext class provides a method named accumulator for creating an accumulator variable.
  * It takes two arguments. The first argument is the initial value for the accumulator and the second argument,
  * which is optional, is a name for displaying in the Spark UI. It returns an instance of the Accumulator
  * class, which provides the operators for working with an accumulator variable. Tasks can only add a value
  * to an accumulator variable using the add method or += operator. Only the driver program can read an
  * accumulator’s value using it value method.
  * */


  /*
  * A numeric accumulator can be created by calling SparkContext.longAccumulator() or SparkContext.doubleAccumulator()
  * to accumulate values of type Long or Double, respectively. Tasks running on a cluster can then add to it using the
  * add method. However, they cannot read its value. Only the driver program can read the accumulator’s value,
  * using its value method.
  *
  * You can write your own accumulator by extending AccumulatorV2 class. For more details,
  * visit https://spark.apache.org/docs/2.2.0/rdd-programming-guide.html#accumulators
  * */

  //named accumulator
  val accum=spark.sparkContext.longAccumulator("My Accumulator")
  val nums=sc.parallelize(Seq(1,2,3,4,5,6,7,8,9,34,57,45,5757,34,34,57,56,91))
  //sum all elements in nums
  nums.foreach(x=>accum.add(x))
  println(s"Accum value is : ${accum.value}")

  case class Customer(id:Long,name:String)
  val customers=sc.parallelize(List(
    Customer(1,"Tom"),
    Customer(2,"Harry"),
    Customer(-1,"Paul")
  ))
  //println(s"customers value ${customers.collect.mkString(",")}")
  val badIds = sc.longAccumulator("Bad id accumulator")
  /*customers.foreach(c=>{c match {
  case Customer(id,name)=>{if(id<0)badIds.add(1)}
  }})*/

  val validCustomers = customers.filter(c => if (c.id < 0) {
    badIds.add(1)
    false
  } else true
  )
  println(s"Good id value: ${validCustomers.count}")
  println(s"badIds value: ${badIds.value}")

  /* Accumulators should be used with caution. Updates to an accumulator within a transformation are not
   * guaranteed to be performed exactly once. If a task or stage is re-executed, each task’s update will be applied
   * more than once.
   *
   * In addition, the update statements are not executed until an RDD action method is called. RDD
   * transformations are lazy; accumulator updates within a transformation are not executed right away.
   * Therefore, if a driver program uses the value of an accumulator before an action is called, it will get the
   * wrong value.*/
}
}
