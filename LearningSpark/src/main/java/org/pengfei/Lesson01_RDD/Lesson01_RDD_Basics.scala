package org.pengfei.Lesson01_RDD

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

/** **************************************************************************************
 * ************************************Introduction ***********************************
 * ************************************************************************************
 *
 * An RDD in spark is simply an immutable distributed collection of objects. Each RDD
 * is split into multiple partitions, which may be computed on different nodes of the cluster.
 * RDDs can contain any type of Python, Java or Scala objects, including user-defined
 * classes.
 *
 * Users create RDDs in two ways: by loading an external dataset, or by distributing a
 * collection of objects (e.g. a list or set) in their driver program.
 * https://spark.apache.org/docs/2.2.0/rdd-programming-guide.html
 *
 * **********************************************************************************/


object Lesson01_RDD_Basics {


  def main(args: Array[String]) = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = SparkSession.builder().
      master("local[4]"). //spark://10.70.3.48:7077 remote
      appName("Lesson1_RDD").
      getOrCreate()
    // import sparkSession.implicits._ for all schema conversion magic.

    //get dynamic data path
    val sparkConfig = ConfigFactory.load("application.conf").getConfig("spark")
    val path = sparkConfig.getString("sourceDataPath")

    RddCreationOperation(spark, path)
    // RddTransformationOperation(spark)
    // RddActionOperations(spark)

    /* The special transformations and actions for RDD with Key value pair and num types and be found
    * in the file Lesson1_RDDWithKeyValuePair.scala and Lesson1_RDDWithNumTypes.scala */

    //RddSaveOperation(spark)
    //RddCachingOperations(spark)
    // RddUdfOperations(spark)


  }


  def RddCreationOperation(spark: SparkSession, path: String): Unit = {
    // get the spark context instance
    val sc = spark.sparkContext
    /** ************************************************************************************************
     * ********************* 1.1 Creating RDDs  ***********************************************************
     * ************************************************************************************************/

    /*
    * Since RDD is an abstract class, you cannot create an instance of the RDD class directly. The SparkContext
    * class provides factory methods to create instances of concrete implementation classes. An RDD can also be
    * created from another RDD by applying a transformation to it. As discussed earlier, RDDs are immutable. Any
    * operation that modifies an RDD returns a new RDD with the modified data.
    *
    * There is three main ways to create an RDD:
    * 1. RDDs creation with in memory collection of objects
    * 2. RDDs creation with files
    * 3. RDDs creation with RDD transformation operations
    *
    * Spark driver will submit a new job to executor when it encounters action. Spark's RDD are by default recomputed
    * each time you run an action on them. If you would like to reuse an RDD in multiple actions, you can ask spark
    * to persist it using RDD.persist(). After computing it the first time Spark will store the RDD contents in
    *  memory(partitioned across the machines in your cluster). Persisting RDDs on disk is also possible.
    *
    * The behavior of not persisting by default may again seem unusual, but it makes a lot of sense for big datasets:
    * if you will not reuse the RDD, there’s no reason to waste storage space when Spark could instead stream
    * through the data once and just compute the result.
    * */

    // print(linesHasPython.count())
    // print(linesHasPython.first())

    /*
    * The ability to always recompute an RDD is actually why RDDs are called "resilient". When a machine holding
    * RDD data fails, Spark uses this ability to recompute the missing partitions, transparent to the user.
    * */
    /** 1.1.1 RDDs creation with in memory collection of objects
     *
     * The first way to create an RDD is to parallelize an object collection, meaning converting it to a
     * distributed dataset that can be operated in parallel. This is a great way to get started in learning
     * Spark because it is simple and doesn’t require any data files.
     *
     * Spark context provide two methods:
     * - parallelize() : It takes a in memory collection and returns an RDD
     * - makeRDD(): It's a warp method of parallelize(). makeRDD calls parallelize() to create RDDs
     *
     * This works for all scala collection types: List, Array, Seq, etc.
     */

    val strCollection1 = List("pandas", "i like pandas")
    val rddFromMem1 = spark.sparkContext.parallelize(strCollection1)
    val linesHasPandas = rddFromMem1.filter(line => line.contains("pandas"))
    print(linesHasPandas.count())
    print(linesHasPandas.first())

    val strCollection2 = Array("spark", "i like spark")
    //numSlices defines the partition number
    val rddFromMem2 = sc.makeRDD(strCollection2,4)
    // persist an rdd in memory
    rddFromMem2.persist(StorageLevel.MEMORY_ONLY_SER)



    /** 1.1.2 RDD creation with external datasets
     * Spark can create RDDs from any storage source supported by Hadoop, including your local file system, HDFS,
     * Cassandra, HBase, Amazon S3, etc. Spark supports text files, SequenceFiles, and any other Hadoop InputFormat.
     *
     * Spark context provide several methods to read file and convert it to RDDs.
     * - textFile(path:String,minPartitions:int): The minPartitions specifies the minimum partition of an RDD
     *          base on the config of the context, spark can make more. By default, spark creates one partition
     *          for each hdfs file block
     * - wholeTextFiles(): This method reads all text files in a directory and returns an RDD of key-value pairs.
     *          Each key-value pair in the returned RDD corresponds to a single file. The key stores the path
     *          of a file and the value part stores the content of a file.
     * - sequenceFile(): The sequenceFile method reads key-value pairs from a sequence file. It returns an RDD of
     *          key-value pairs. In addition to providing the name of an input file, you have to specify the data
     *          types for the keys and values as type parameters when you call this method
     *
     * textFile vs wholeTextFiles
     * If we use wildcard path argument in these two methods, they will both read multiple files and return an RDD.
     * In the RDD returned by textFile, each line in a file will be an element of the rdd. So you can't determine
     * which line is form which file. wholeTextFile() will return a rdd of type key value pair, key is the file path,
     * value is the list of lines of the file.
     *
     * */

    /** 1.1.2.1 textFile code example */

    val readMeFilePath = s"${path}/spark_lessons/Lesson01_RDD/README.md"
    val readMeLines = sc.textFile(readMeFilePath,4)
    val linesHasPython = readMeLines.filter(line => line.contains("python"))
    print(linesHasPython.first())



    /** 1.1.2.2 wholeTextFiles code example */


    val allRdd = sc.textFile(s"${path}/spark_lessons/Lesson01_RDD/*.txt")
    val allRddFromLessson1 = sc.wholeTextFiles("/home/pliu/data_set/spark_data_set/spark_lessons/Lesson01_RDD/*.txt")
    /* */
    println(s"rdd with textFile : ${allRdd.collect().toList.toString()}")
    println(s"rdd with wholeTextFile : ${allRddFromLessson1.collect().toMap.toString()}")

    /** 1.1.2.3 sequenceFile code example */


    //val rdd = sc.sequenceFile[String, String]("SequenceFilePath")


    /** 1.1.3 RDD creation with transformation
     * There are many RDD transformation operations which can create new RDDs. I only give two example:
     * - Exp1: We convert a dataFrame to RDD.
     * - Exp2: We use a map() operation, which transform an RDD of lines of a file (RDD[String]) to an RDD of class
     *         clientSatisfied(RDD[clientSatisfied]). Note class client_satisfied is opaque to RDD.
     */

    /** 1.1.3.1 Exp1: RDD creation from dataframe */

    val satisfiedClientFilePath=s"${path}/spark_lessons/Lesson01_RDD/satisfait_client.data"
    val df = spark.read.format("csv").option("header", "true").load(satisfiedClientFilePath)
    df.show()
    val dfToRdd = df.rdd
    println(s"rdd value : ${dfToRdd.collect().toList.toString}")

    /** 1.1.3.2 Exp2: Transform RDD with map */
    case class clientSatisfied(manager_name: String, client_name: String, client_gender: String,
                                 client_age: Int, response_time: Double, statisfaction_level: Double)
    val client_sat_lines = sc.textFile(satisfiedClientFilePath)
    val satisfait_client = client_sat_lines.map { l => {
      // isEmpty is a string specific method for null value testing, because ==null does not work in scala
      if (l.isEmpty) {
        println("This is an empty line")
      }
      else {
        val s = l.split(",")
        // s is not an rdd , l is a string, l.split returns an array of String
        // println(s"type of line after split: ${s.getClass.getName}")
        clientSatisfied(s(0), s(1), s(2), s(3).toInt, s(4).toDouble, s(5).toDouble)
      }
    }
    }
    // Below code is illegal, because class clientSatisfied is opaque for RDD.
    // satisfait_client.first().manager_name
    println(s" satisfait_client_rdd value is ${satisfait_client.collect().mkString(",")}")
  }

  /** **********************************************************************************************
   * ************************************* 1.2 RDDs operations : transformation******************
   * ***************************************************************************************************/

  def RddTransformationOperation(spark: SparkSession): Unit = {

    /*
    * RDDs transformation are all lazy in spark, transformation does not mutate the existing inputRDD. Instead,
    * it returns a pointer to an entirely new RDD.
    *
    * RDD transformations are conceptually similar to Scala collection methods. The key difference is that
    * the Scala collection methods operate on data that can fit in the memory of a single machine, whereas RDD
    * methods can operate on data distributed across a cluster of nodes.
    *
    * In index 1, you can find all rdd transformation list, here we just illustrate some important transformation
    * */


    /** *************************************** Map and MapPartitions *****************************************/
    /* Map method is a higher-order method that takes a functions as input and applies it to each element
    * in the source RDD to create a new RDD. The input function must take a signle argument and return a
    * signle value */
    val numRDD = spark.sparkContext.parallelize(List(1, 2, 3, 4))
    val square = numRDD.map(x => x * x)
    println("Square of each element of RDD done by Map: " + square.collect().mkString(","))

    /* mapPartitions method allows you to process data at a partition level, Instead of passing one
     * element at a time to its input function, mapPartitions passes a partition in the form of an
     * iterator. The input function to the mapPartitions method takes an iterator as input and returns
     * another iterator as output. The mapPartitions method returns new RDD formed by applying a
     * user-specified function to each partition of the source RDD.*/
    val squarePartMap = numRDD.mapPartitions(iter => iter.map { x => x * x })
    println("Square of each element of RDD done by MapPartitions: " + squarePartMap.collect().mkString(","))

    /** ************************************* Flatmap *****************************************/
    /* Flat map is similar to map, the difference is flatmap will flat the content of the rdd.
    * For example map(line=>line.split(" ") will generate a list1(list11(),list12()) list1 is the
    * list of all lines, list11 is the list of words in line number 1 of all lines
    *
    * flatMap will generate a list1(word1,word2,....) list1 is the list of all words*/
    val textRDD = spark.sparkContext.parallelize(List("hello world", "hi"))
    val words = textRDD.flatMap(line => line.split(" "))
    println(s"Flat map of words value : ${words.collect().mkString(",")}")

    /** ****************************************Filter *******************************************/
    /* The filter method takes a Boolean function as input and applies it to each element in the source
     * RDD to create a new RDD. The filter method returns a new RDD formed by selecting only those elements
     * for which the input Boolean function returned true. Thus, the new RDD contains a subset of the elements
     * in the original RDD*/
    val logRDD = spark.sparkContext.textFile("/home/pliu/data_set/spark_data_set/spark_lessons/Lesson01_RDD/log.log")
    val errorRDD = logRDD.filter(line => line.contains("ERROR"))
    val warnRDD = logRDD.filter(line => line.contains("WARN"))

    println(s"error lines value: ${errorRDD.first()}")
    println(s"warn lines value: ${warnRDD.first()}")

    /** ****************************** Union, intersection and subtract *******************************/
    val badLineRDD = errorRDD.union(warnRDD)
    val num1 = spark.sparkContext.parallelize(List(1, 2, 3))
    val num2 = spark.sparkContext.parallelize(List(3, 4, 5))
    /* The union method takes an RDD as input and returns a new RDD that contains the union of the elements in
     * the source RDD and the RDD passed to it as an input. C=A.union(B), c in C means c in A or in B*/
    val numUnion = num1.union(num2)
    println(s" The union of two num set is : ${numUnion.collect().mkString(",")}")
    /* The intersection C=A.intersection(B), c in C means c in A and c in B*/
    val numIntersection = num1.intersection(num2)
    println(s" The intersection of two num set is : ${numIntersection.collect().mkString(",")}")
    /* The subtract method takes an RDD as input and returns a new RDD that contains elements in the source
     * RDD but not in the input RDD. C=A.subtract(B), c in C means c in A and c not in B.
      * */
    val numSubtract = num1.subtract(num2)
    println(s" The subtract of two num set is : ${numSubtract.collect().mkString(",")}")

    /** ********************************* Cartesian **********************************/
    /* The cartesian method of an RDD takes an RDD as input and returns an RDD containing the cartesian
    * product of all the elements in both RDDs. It returns an RDD of ordered pairs, in which the first element
    * comes from the source RDD and the second element is from the input RDD. The number of elements in the
    * returned RDD is equal to the product of the source and input RDD lengths.*/
    val numCartesian = num1.cartesian(num2)

    /** ***************************** zip and zipWithIndex **************************/
    /* The zip method takes an RDD as input and returns an RDD of pairs, where the first element in a pair is
     * from the source RDD and second element is from the input RDD. Unlike the cartesian method, the RDD
     * returned by zip has the same number of elements as the source RDD. Both the source RDD and the input
     * RDD must have the same length. In addition, both RDDs are assumed to have same number of partitions
     * and same number of elements in each partition.*/

    val numbers = spark.sparkContext.parallelize(List(1, 2, 3, 4))
    val alphabets = spark.sparkContext.parallelize(List("a", "b", "c", "d"))
    val zipPaires = numbers.zip(alphabets)
    println(s"zipPaires value is : ${zipPaires.collect().mkString(",")}")

    /* The zipWithIndex method zips the elements of the source RDD with their indices and returns an RDD of pairs.*/
    val alphabetsWithIndex = alphabets.zipWithIndex()
    println(s"zipPairesWithIndex value is : ${alphabetsWithIndex.collect().mkString(",")}")

    /** ***************************** groupBy ***********************************/
    /* groupBy method groups the elements of an RDD according to a user specified criteria. It takes as input a function
    * that generates a key for each element in the source RDD. It applies this function to all the elements in the source
    * RDD and returns an RDD of pairs. In each returned pair, the first item is a key and the second item is a collection
    * of the elements mapped to that key by the input function to the groupBy method.
    *
    * Note that the groupBy method is an expensive operation since it may shuffle data.*/

    /*Following rdd is the report of client satisfacation of a company's service*/

    case class client_satisfait(manager_name: String, client_name: String, client_gender: String, client_age: Int, response_time: Double, statisfaction_level: Double)
    val client_sat_lines = spark.sparkContext.textFile("/home/pliu/data_set/spark_data_set/spark_lessons/Lesson01_RDD/satisfait_client.data")
    val client_rdd = client_sat_lines.map { l => {
      if (l.isEmpty) {
        println("This is an empty line")
      }
      else {
        val s = l.split(",")
        client_satisfait(s(0), s(1), s(2), s(3).toInt, s(4).toDouble, s(5).toDouble)
      }
    }
    }
    // println(s"client_rdd type: ${client_rdd.getClass.getName}")
    // println(s"Sample of client_rdd : ${client_rdd.first()} ")
    val groupByAge = client_rdd.groupBy(c => {
      c match {
        case client_satisfait(manager_name, client_name, client_gender, client_age, response_time, statisfaction_level) => client_age
        case _ => 0
      }
    })

    //println(s"groupByAge value is : ${groupByAge.collect().mkString(",")}")

    /** ****************************KeyBy *************************************/
    /* The keyBy method is similar to the groupBy method. It a higher-order method that takes as input a function
     * that returns a key for any given element in the source RDD. The keyBy method applies this function to all the
     * elements in the source RDD and returns an RDD of pairs. In each returned pair, the first item is a key which is
     * calculated by using the input function and the second item is an element that was mapped to that key by the
     * input function to the keyBy method. The RDD returned by keyBy will have the same number of elements as the
     * source RDD*/

    val keyByAge = client_rdd.keyBy(c => {
      c match {
        case client_satisfait(manager_name, client_name, client_gender, client_age, response_time, statisfaction_level) => client_age
        case _ => 0
      }
    })

    // println(s"KeyByAge value is : ${keyByAge.collect().mkString(",")}")
    /** **************************************sortBy ********************************/
    /* The sortBy method returns an RDD with sorted elements from the source RDD. It takes two input parameters.
     * The first input is a function that generates a key for each element in the source RDD. The second argument
     * allows you to specify ascending or descending order for sort.*/

    val unSortedNums = spark.sparkContext.parallelize(List(4, 9, 1, 5, 8, 3, 2))
    val sortedNums = unSortedNums.sortBy(x => x, true)
    //println(s"sortedNums value is : ${sortedNums.collect.mkString(",")}")

    val sortByAge = client_rdd.sortBy(c => {
      c match {
        case client_satisfait(manager_name, client_name, client_gender, client_age, response_time, statisfaction_level) => client_age
        case _ => 0
      }
    })
    //println(s"client satisfait rdd sortByAge value is : ${sortByAge.collect.mkString("|")} ")

    /** ******************************** pipe *****************************************************/
    /* The pipe method allows you to execute an external program in a forked process. It captures the output of the
    * external program as a String and returns an RDD of Strings.*/

    /** ****************************** randomSplit ****************************************************/
    /* The randomSplit method splits the source RDD into an array of RDDs. It takes the weights of the splits as input.*/
    val numbersTobeSplited = spark.sparkContext.parallelize((1 to 100).toList)
    val splittedNumbers = numbersTobeSplited.randomSplit(Array(0.8, 0.2))
    val firstSet = splittedNumbers(0)
    val secondSet = splittedNumbers(1)
    println(s"FirstSet has ${firstSet.count()}, Second set has ${secondSet.count()}")

    /** *******************************coalesce ****************************************/
    /* The coalesce method reduces the number of partitions in an RDD. It takes an integer input and returns a new RDD
    * with the specified number of partitions. This function is very useful when you want to export your result to
    * normal file system (not HDFS). So you can have all your result in one file not in 100 files. */
    val allNumsInOnePartitions = numbersTobeSplited.coalesce(1, true)

    /** ******************************repartition *************************************/
    /* The repartition method takes an integer as input and returns an RDD with specified number of partitions.
    * It is useful for increasing parallelism. It redistributes data, so it is an expensive operation.
    * The coalesce and repartition methods look similar, but the first one is used for reducing the number
    * of partitions in an RDD, while the second one is used to increase the number of partitions in an RDD.
    *
    * Note that the repartition has limit, for example, if you have a RDD with 3 element, you can't repartion it to
    * have 4 partitions. And the size of each partition must be smaller than 2GB*/
    val numsWithSixPartitions = numbersTobeSplited.repartition(6)

    /** ***************************** sample ****************************************/
    /* The sample method returns a sampled subset of the source RDD. It takes three input parameters. The first
    * parameter specifies the replacement strategy. The second parameter specifies the ratio of the sample size to
    * source RDD size. The third parameter, which is optional, specifies a random seed for sampling. if the seed is a fixed
    * value, the sample can be reproduced */
    val seed: Long = 0
    val sampleOfNums = numbersTobeSplited.sample(true, 0.2, seed)

  }


  /** **********************************************************************************************
   * ************************************* 1.3 RDDs operations : Actions **********************
   * ***************************************************************************************************/

  /* Actions are RDD methods that return a value to a driver program. Unlike transformations are lazy, Actions will be
  * executed when it appears*/
  def RddActionOperations(spark: SparkSession): Unit = {

    /** ********************** RDDs Actions examples **********************/
    val num1 = spark.sparkContext.parallelize(List(1, 2, 3, 3, 2, 1, 5, 6, 7))
    val num2 = spark.sparkContext.parallelize(List(3, 4, 5))

    /** ****************************** Collect ****************************************/
    /* The collect method returns the elements in the source RDD as an array. This method should be used with
    * caution since it moves data from all the worker nodes to the driver program. It can crash the driver program
    * if called on a very large RDD.*/
    val squareNumRdd = num1.map(x => x * x)
    val squareNumRes = squareNumRdd.collect()
    println(s"squareNumRes has type : ${squareNumRes.getClass().getName()}")
    println(s"squareNumRes has value: ${squareNumRes.mkString(",")}")

    /** ***********************count and countByValue **********************************/
    /* The count method returns a count(long type) of the elements in the source RDD.*/

    val countNum1 = num1.count()
    println(s"countNum1 has type: ${countNum1.getClass.getName}")
    println(s"countNum1 has value: ${countNum1}")
    /* The countByValue method returns a count of each unique element in the source RDD. It returns an instance
     * of the Map class containing each unique element and its count as a key-value pair.*/

    val countByValueNum1 = num1.countByValue()
    println(s"countByValueNum1 has value: ${countByValueNum1.mkString(",")}")

    /** ******************first, take, takeOrdered, top *******************************/
    /* The first method returns the first element in the source RDD. */
    val firstElementOfNum1 = num1.first()
    println(s"firstElementOfNum1 is : ${firstElementOfNum1}")

    /* The top method takes an integer N as input and returns an array containing the N largest elements
    * in the source RDD.*/

    val biggestThreeElementOfNum1 = num1.top(3)
    println(s"BiggestThreeElementOfNum1 has value: ${biggestThreeElementOfNum1.mkString(",")}")

    /* The take method takes an integer N as input and returns an array containing the first N element in the
     * source RDD.*/

    val firstThreeElementOfNum1 = num1.take(3)
    println(s"firstThreeElementOfNum1 has value: ${firstThreeElementOfNum1.mkString(",")}")

    /* The take method takes an integer N as input and returns an array containing the first N element in the
     * source RDD.*/

    val smallestThreeElementOfNum1 = num1.takeOrdered(3)
    println(s"smallestThreeElementOfNum1 has value: ${smallestThreeElementOfNum1.mkString(",")}")

    /** *************************Min, Max **********************************************/
    /* The min method returns the smallest element in an RDD. The max method returns the largest element in an RDD.
    * For rdd of strings. It did return a value, but it's not right*/
    val minNum = num1.min()
    val maxNum = num1.max()
    println(s"minNum has value ${minNum}, maxNum has value ${maxNum}")


    /** *********************************************** Reduce ******************************/

    /* The higher-order reduce method aggregates the elements of the source RDD using an associative and
     * commutative binary operator provided to it. It is similar to the fold method; however, it does not require a
     * neutral zero value.
     *
     * associative means (a op b) op c = a op (b op c)
     * commutative means a op b = b op a
     *
     * + is associative and commutative
     * / is neither associative nor commutative
     * */
    val sumReduce = num1.reduce((x, y) => x + y)
    println(s"Sum of num1 done by reduce: ${sumReduce}")

    /** ******************************************* fold ***********************************/

    /* The higher-order fold method aggregates the elements in the source RDD using the specified neutral zero
     * value and an associative binary operator. It first aggregates the elements in each RDD partition and then
     * aggregates the results from each partition.
     *
     * The neutral zero value depends on the RDD type and the aggregation operation. For example, if you
     * want to sum all the elements in an RDD of Integers, the neutral zero value should be 0. Instead, if you want
     * to calculate the products of all the elements in an RDD of Integers, the neutral zero value should be 1.*/

    /*
    *  fold[T](acc:T)((acc,value)=>acc)
    *  - T is the data type of RDD
    *  - acc is accumulator
    *  - value is the elements of RDDs
    *  To start fold we must give a initial acc value which has the same data type as RDD elements.
    * */
    val maxByFold = num1.fold(0)((acc, num) => {
      if (acc < num) num else acc
    })
    println(s"Max value done by fold ${maxByFold}")

    val productByFold = num1.fold(1)((acc, num) => {
      acc * num
    })
    println(s"Product value done by fold ${productByFold}")


    /** *************************************Aggregate **********************************************/
    /*
    * The aggregate function frees us from the constraint of having the return be the same type as the RDD which
    * we are working on. With aggregate, like fold, we supply an initial zero value of the type we want to return.
    * We then supply a function to combine the elements from our RDD with the accumulator. Finally, we need to
    * supply a second function to merge two accumulators, given that each node accumulates its own results locally.
    * */
    /*The last argument 4 means the numbers of partitions of the rdd */
    val flowers = spark.sparkContext.parallelize(List(11, 12, 13, 24, 25, 26, 35, 36, 37, 24, 15, 16), 4)
    //println("Flowers rdd partions number "+flowers.partitions.size)

    /*_+_ means sum of the args (x,y)=>x+y */
    /*The fist function calculate with all elements inside the partition(Intra-partition)
    * The second function calculate with the result of the first fonction(Inter-partition) */
    val flowerSum = flowers.aggregate(0)((x, y) => x + y, (x, y) => x + y)
    /*If the initial accumulator is not 0, the value will be add in each function*/
    val simpleFlowerSum = flowers.aggregate(2)(_ + _, _ + _)
    println("Sum done by aggregate " + flowerSum)
    println("Sum done by aggregate " + simpleFlowerSum)

  }

  /** **********************************************************************************************
   * ************************************* 1.4 Saving RDDs  **********************
   * ***************************************************************************************************/
  def RddSaveOperation(spark: SparkSession): Unit = {
    /* Generally, after data is processed, results are saved on disk. Spark allows an application developer to save
     * an RDD to any Hadoop-supported storage system. An RDD saved to disk can be used by another Spark or
     * MapReduce application.*/

    /** ********************************Save as textFile *****************************************/

    /* The saveAsTextFile method saves the elements of the source RDD in the specified directory on any
     * Hadoop-supported file system. Each RDD element is converted to its string representation and stored as a
     * line of text*/

    val saveRdd = spark.sparkContext.parallelize(List("orange", "apple", "banana", "peach", "kiwi"))
    /* Here I save it on a local file system. if you want to write to hdfs, you need to change file:// to hdfs://
    * And if you want to write all result in a signle part file, you need to change the partion of the rdd. Because
    * for each partion, spark witl write a part file in hdfs. */
    saveRdd.saveAsTextFile("file:///tmp/saveRDDTest")
    //change rdd partions to 1 then save it to hdfs
    //saveRdd.coalesce(1).saveAsTextFile("hdfs://path")

    /** ************************************Save as objectFile *****************************************/
    /* The saveAsObjectFile method saves the elements of the source RDD as serialized Java objects in
    * the specified directory. */
    saveRdd.saveAsObjectFile("file:///tmp/saveRDDObjectTest")

    /** ***********************************Save as sequence file ****************************************/
    /* The saveAsSequenceFile method saves an RDD of key-value pairs in SequenceFile format. An RDD of keyvalue
     * pairs can also be saved in text format using the saveAsTextFile */
    val pairRdd = saveRdd.map(x => (x, x.contains("a")))
    //saveRdd.saveAsSequenceFile("Path") does not work, because it's not a pair rdd
    pairRdd.saveAsSequenceFile("file:///tmp/saveRDDSeqTest")

    /* Note that all of the preceding methods take a directory name as an input parameter and create one file
     * for each RDD partition in the specified directory. This design is both efficient and fault tolerant. Since each
     * partition is stored in a separate file, Spark launches multiple tasks and runs them in parallel to write an RDD
     * to a file system. It also helps makes the file writing process fault tolerant. If a task writing a partition
     * to a file fails, Spark creates another task, which rewrites the file that was created by the failed task.*/

  }

  /** **********************************************************************************************
   * ************************************* 1.5 Caching RDDs (Persistence) **********************
   * *************************************************************************************************/

  /*
  * One of the most important capabilities in Spark is persisting (or caching) a dataset in memory across operations.
  * By default, when an action method of an RDD is called, Spark creates that RDD from its parents, which may
  * require creation of the parent RDDs, and so on. This process continues until Spark gets to the root RDD,
  * which Spark creates by reading data from a storage system. This happens every time an action method is
  * called. Thus, by default, every time an action method is called, Spark traverses the lineage tree of an RDD
  * and computes all the transformations to obtain the RDD whose action method was called.
  *
  * For example. the following code will read textfile from disk twice, even tough there are only one line for textFile
  *
  * val logs=sc.textFile("path/to/log-files")
  * val warningLogs= logs.filter{l=>l.contains("WARN")}
  * val errorLogs=logs.filter{l=>l.contains("ERROR")}
  * val warnCount=warningLogs.count
  * val errorCount=errorLogs.count
  *
  * !!!!!!!!!!!!!!!!!!!!!!!!!!!!caching is lazy!!!!!!!!!!!!!!!!!!!!!!
  * Caching rdd will wait the first time an action is called on the cached RDD to store it in memory. After caching,
  * Spark stores it in the executor memory on each worker node. Each executor stores in memory the RDD partitions
  * that it computes.
  *
  * So If an rdd will be used many times in a spark application (iterative algorithms), Caching it can make
  * the performence better.
  *
  * When you persist an RDD, each node stores any partitions of it that it computes in memory and reuses them in other
  * actions on that dataset (or datasets derived from it). This allows future actions to be much faster
  * (often by more than 10x).
  *
  * You can mark an RDD to be persisted using the persist() or cache() methods on it. The persist method is a generic
  * version of the cache method. It allows an RDD to be stored in memory, disk, or both. Cache method can only store in
  * memory.
  *
  * Spark’s cache is fault-tolerant – if any partition of an RDD is lost, it will automatically be recomputed using
  * the transformations that originally created it.
  *
  *
  * In addition, each persisted RDD can be stored using a different storage level, allowing you, for example,
  * to persist the dataset on disk, persist it in memory but as serialized Java objects (to save space), replicate it
  * across nodes. These levels are set by passing a StorageLevel object (Scala, Java, Python) to persist().
  * The cache() method is a shorthand for using the default storage level, which is StorageLevel.MEMORY_ONLY
  * (store deserialized objects in memory).
  * */
  def RddCachingOperations(spark: SparkSession): Unit = {
    val logPath = "/home/pliu/data_set/spark_data_set/spark_lessons/Lesson01_RDD/log.log"
    val logs = spark.sparkContext.textFile(logPath)
    val errorsAndWarnings = logs filter { l => l.contains("ERROR") || l.contains("WARN") }
    //cache in memory
    errorsAndWarnings.cache()
    // The following code are persist code example
    //errorsAndWarnings.persist(StorageLevel.MEMORY_ONLY)
    //errorsAndWarnings.persist(StorageLevel.MEMORY_AND_DISK)
    //Store RDD as seriallized java object, it uses less memory, but more cpu intensive to read.
    //errorsAndWarnings.persist(StorageLevel.MEMORY_AND_DISK_SER_2)
    val errorLogs = errorsAndWarnings.filter(l => l.contains("ERROR"))
    val warnLogs = errorsAndWarnings.filter(l => l.contains("WARN"))
    val errorCount = errorLogs.count()
    val warnCount = warnLogs.count()


    /* Cache Memory Management
     * Spark automatically manages cache memory using LRU (least recently used) algorithm. It removes old
     * RDD partitions from cache memory when needed. In addition, the RDD API includes a method called
     * unpersist(). An application can call this method to manually remove RDD partitions from memory.*/
  }


  /** **********************************************************************************************
   * ****************** 1.6 Use user define function(udf) in transformation and action******************
   * *************************************************************************************************/
  def RddUdfOperations(spark: SparkSession): Unit = {
    /*
   *
    * Spark’s API relies heavily on passing functions in the driver program to run on the cluster. There are two
    * recommended ways to do this:
    *    1. Anonymous function syntax, which can be used for short pieces of code.(lamda exp)
    *    2. Static methods in a global singleton object.
    *
    * For example, you can define object addTextToLines and method addEat and then use them in a map, as follows: */
    val fruits = spark.sparkContext.parallelize(List("orange", "apple", "banana", "wiki"))
    val eatFruits = fruits.map(line => addTextToLines.addEat(line))
    println(s"eatFruits value: ${eatFruits.collect.mkString(",")}")

    /* You can also call directly a function define inside the same object*/
    val throwF = fruits.map(line => throwFruits(line))
    println(s"throwF value: ${throwF.collect.mkString(",")}")

    /*All transformation and actions which take a function as argument can use udf also*/
  }

  object addTextToLines {
    def addEat(line: String): String = {
      return "Eat " + line
    }
  }

  def throwFruits(line: String): String = {
    return "Throw " + line
  }

  /** *****************************************************************************************************************
   * ************************************** Index ******************************************************************
   * ***********************************************************************************************************/

  /* Index 1
    * RDDs transformation List:
    *     - map(func):	Return a new distributed dataset formed by passing each element of the source through a function func.
    *
    *     - flatMap(func):	Similar to map, but each input item can be mapped to 0 or more output items (so func should
    *                     return a Seq rather than a single item).
    *
    *     - mapPartitions(func):	Similar to map, but runs separately on each partition (block) of the RDD, so func
    *                          must be of type Iterator<T> => Iterator<U> when running on an RDD of type T.
    *
    *     - filter(func):	Return a new dataset formed by selecting those elements of the source on which func returns true.
    *
    *     - sample(withReplacement, fraction, seed):	Sample a fraction fraction of the data, with or without
    *                                          replacement, using a given random number generator seed.
    *
    *     - mapPartitionsWithIndex(func):	Similar to mapPartitions, but also provides func with an integer value
    *                                     representing the index of the partition, so func must be of type
    *                                     (Int, Iterator<T>) => Iterator<U> when running on an RDD of type T.
    *
    *     - union(otherDataset):	Return a new dataset that contains the union of the elements in the source dataset
    *                           and the argument.
    *
    *     - intersection(otherDataset):	Return a new RDD that contains the intersection of elements
    *                                   in the source dataset and the argument.
    *
    *     - distinct([numTasks])):	Return a new dataset that contains the distinct elements of the source dataset.
    *
    *     - groupByKey([numTasks]):	 When called on a dataset of (K, V) pairs, returns a dataset of
    *                                (K, Iterable<V>) pairs.
    *                                Note: If you are grouping in order to perform an aggregation
    *                                (such as a sum or average) over each key, using reduceByKey or aggregateByKey
    *                                will yield much better performance.
    *                                Note: By default, the level of parallelism in the output depends on the number
    *                                of partitions of the parent RDD. You can pass an optional numTasks argument to
    *                                set a different number of tasks.
    *
    *    - reduceByKey(func, [numTasks])	When called on a dataset of (K, V) pairs, returns a dataset of (K, V) pairs
    *                                     where the values for each key are aggregated using the given reduce function
    *                                     func, which must be of type (V,V) => V. Like in groupByKey, the number of
    *                                     reduce tasks is configurable through an optional second argument.
    *
    *   - aggregateByKey(zeroValue)(seqOp, combOp, [numTasks])	When called on a dataset of (K, V) pairs, returns a
    *                                                           dataset of (K, U) pairs where the values for each key
    *                                                           are aggregated using the given combine functions and a
    *                                                           neutral "zero" value. Allows an aggregated value type
    *                                                           that is different than the input value type, while
    *                                                           avoiding unnecessary allocations. Like in groupByKey,
    *                                                           the number of reduce tasks is configurable through
    *                                                           an optional second argument.
    *
    *   - sortByKey([ascending], [numTasks])	When called on a dataset of (K, V) pairs where K implements Ordered,
    *                                         returns a dataset of (K, V) pairs sorted by keys in ascending or
    *                                         descending order, as specified in the boolean ascending argument.
    *
    *   -join(otherDataset, [numTasks])	When called on datasets of type (K, V) and (K, W), returns a dataset of
    *                                   (K, (V, W)) pairs with all pairs of elements for each key. Outer joins are
    *                                   supported through leftOuterJoin, rightOuterJoin, and fullOuterJoin.
    *
    *   - cogroup(otherDataset, [numTasks])	When called on datasets of type (K, V) and (K, W), returns a dataset of
    *                                      (K, (Iterable<V>, Iterable<W>)) tuples. This operation is also called groupWith.
    *
    *   - cartesian(otherDataset)	When called on datasets of types T and U, returns a dataset of (T, U) pairs (all pairs of elements).
    *
    *   - pipe(command, [envVars])	Pipe each partition of the RDD through a shell command, e.g. a Perl or bash script.
    *                               RDD elements are written to the process's stdin and lines output to its stdout are
    *                               returned as an RDD of strings.
    *
    *   - coalesce(numPartitions)	Decrease the number of partitions in the RDD to numPartitions. Useful for running
    *                             operations more efficiently after filtering down a large dataset.
    *
    *   - repartition(numPartitions)	Reshuffle the data in the RDD randomly to create either more or fewer partitions
    *                                 and balance it across them. This always shuffles all data over the network.
    *
    *   - repartitionAndSortWithinPartitions(partitioner)	Repartition the RDD according to the given partitioner and,
    *                                                     within each resulting partition, sort records by their keys.
    *                                                     This is more efficient than calling repartition and then
    *                                                     sorting within each partition because it can push the sorting
    *                                                     down into the shuffle machinery.
    *
    * */


  /* Index 2.
    * RDDs actions list:
    * - reduce(func):	Aggregate the elements of the dataset using a function func (which takes two arguments and
    *                 returns one). The function should be commutative and associative so that it can be computed
    *                 correctly in parallel.
    *
    * - top(n): return the top n element of the dataset
    *
    * - collect():	Return all the elements of the dataset as an array at the driver program. This is usually useful
    *               after a filter or other operation that returns a sufficiently small subset of the data.
    *
    * - count()	Return the number of elements in the dataset.
    *
    * - first()	Return the first element of the dataset (similar to take(1)).
    *
    * - take(n)	Return an array with the first n elements of the dataset.
    *
    * - takeSample(withReplacement, num, [seed])	Return an array with a random sample of num elements of the dataset,
    *                                             with or without replacement, optionally pre-specifying a random number
    *                                             generator seed.
    *
    * - takeOrdered(n, [ordering])	Return the first n elements of the RDD using either their natural order or a
    *                               custom comparator.
    *
    * - saveAsTextFile(path)	Write the elements of the dataset as a text file (or set of text files) in a given
    *                         directory in the local filesystem, HDFS or any other Hadoop-supported file system.
    *                         Spark will call toString on each element to convert it to a line of text in the file.
    *
    * - saveAsSequenceFile(path) (Java and Scala)	Write the elements of the dataset as a Hadoop SequenceFile in a
    *                            given path in the local filesystem, HDFS or any other Hadoop-supported file system.
    *                            This is available on RDDs of key-value pairs that implement Hadoop's Writable interface.
    *                            In Scala, it is also available on types that are implicitly convertible to Writable
    *                            (Spark includes conversions for basic types like Int, Double, String, etc).
    *
    * - saveAsObjectFile(path) (Java and Scala)	Write the elements of the dataset in a simple format using Java
    *                          serialization, which can then be loaded using SparkContext.objectFile().
    *
    * - countByKey()	Only available on RDDs of type (K, V). Returns a hashmap of (K, Int) pairs with the count of each key.
    *
    * - foreach(func)	Run a function func on each element of the dataset. This is usually done for side effects such
    *                 as updating an Accumulator or interacting with external storage systems. Note: modifying variables
    *                 other than Accumulators outside of the foreach() may result in undefined behavior. See
    *                 Understanding closures for more details.
    *
    * */


  /* Index 3.
    * The full set of storage levels is:
    * MEMORY_ONLY	-> Store RDD as deserialized Java objects in the JVM. If the RDD does not fit in memory,
    *                some partitions will not be cached and will be recomputed on the fly each time they're needed.
    *                This is the default level.
    * MEMORY_AND_DISK	-> Store RDD as deserialized Java objects in the JVM. If the RDD does not fit in memory,
    *                    store the partitions that don't fit on disk, and read them from there when they're needed.
    * MEMORY_ONLY_SER -> (Java and Scala)	Store RDD as serialized Java objects (one byte array per partition).
    *                    This is generally more space-efficient than deserialized objects, especially when using a
    *                    fast serializer, but more CPU-intensive to read.
    * MEMORY_AND_DISK_SER -> (Java and Scala)	Similar to MEMORY_ONLY_SER, but spill partitions that don't fit in
    *                        memory to disk instead of recomputing them on the fly each time they're needed.
    * DISK_ONLY	-> Store the RDD partitions only on disk.
    * MEMORY_ONLY_2, MEMORY_AND_DISK_2, etc.	-> Same as the levels above, but replicate each partition on
    *                                            two cluster nodes.
    * OFF_HEAP (experimental)	-> Similar to MEMORY_ONLY_SER, but store the data in off-heap memory.
    *                            This requires off-heap memory to be enabled.
    * */
}
