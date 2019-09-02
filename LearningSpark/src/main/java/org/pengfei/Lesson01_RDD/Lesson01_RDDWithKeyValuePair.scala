package org.pengfei.Lesson01_RDD

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/*
* Key value pairs RDDs are a useful building block in many program
* */
object Lesson01_RDDWithKeyValuePair {
def main(args:Array[String]):Unit={
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val spark = SparkSession.builder().
    master("local[2]"). //spark://10.70.3.48:7077 remote
    appName("Lesson1_PaireRDD").
    getOrCreate()
  // import sparkSession.implicits._ for all schema conversion magic.

 //PaireRDDTransformationOperation(spark)
  PaireRDDActionOperation(spark)

}

  /*****************************************************************************************
    * ********************************* Paire RDD transformations **************************
    * **************************************************************************************/
  /* As paire rdd add key value notion to each element, we can do some transformation on keys and values of the paire*/

  def PaireRDDTransformationOperation(spark:SparkSession):Unit={
    val inventory=spark.sparkContext.parallelize(List(("coffee",1),
      ("coffee",2),("soda",4),("soda",8),("alcool",1),("alcool",1)))

    println(s"Inventory contents : ${inventory.collect.mkString(",")}")


/**********************Keys ******************************************************/
  /* The keys method returns an RDD of only the keys in the source RDD. */
  val keyOfInventory=inventory.keys
    println(s"Inventory keys : ${keyOfInventory.collect.mkString(",")}")

    /********************************* values ***********************************/
    /* The values method returns an RDD of only the values in the source RDD. */
    val valueOfInventory=inventory.values
    println(s"Inventory values : ${valueOfInventory.collect.mkString(",")}")

    /********************************mapValues ********************************/
    /* The mapValues method is a higher-order method that takes a function as input and applies it to each value
     * in the source RDD. It returns an RDD of key-value pairs. It is similar to the map method, except that it applies
     * the input function only to each value in the source RDD, so the keys are not changed. The returned RDD has
     * the same keys as the source RDD.*/
    val squareInventoryValues=inventory.mapValues{x=>x*x}
    println(s"Inventory contents after square value : ${squareInventoryValues.collect.mkString(",")}")

    /*********************************Filter on keys and values****************************/
      /* We can use ._1 to access key and ._2 to access values, if the value is an complexe data structure we can
      * use match case pattern to get the value*/
      val productOfSoda=inventory.filter(x=>x._1.equals("soda"))
    println(s"Products which are sodas: ${productOfSoda.collect().mkString(",")}")
    val productNumMoreThan4=inventory.filter(x=>x._2>4)
    println(s"Products which has more than 4 items ${productNumMoreThan4.collect().mkString(",")}")

    /************************************* join **************************************/
/* The join method takes an RDD of key-value pairs as input and performs an inner join on the source and input
* RDDs. It returns an RDD of pairs, where the first element in a pair is a key found in both source and input RDD
* and the second element is a tuple containing values mapped to that key in the source and input RDD.*/
    val pair1=spark.sparkContext.parallelize(List(("a",1),("b",2),("c",3)))
    val pair2=spark.sparkContext.parallelize(List(("b","two"),("c","three"),("d","four")))
    val joinPair=pair1.join(pair2)
    println(s" Join result of two pairs : ${joinPair.collect.mkString(",")}")

    /**************LeftOuterJoin, rightOuterJoin and fullOuterJoin******************/
    /* The rightOuterJoin method takes an RDD of key-value pairs as input and performs a right outer join on the
     * source and input RDD. It returns an RDD of key-value pairs, where the first element in a pair is a key from
     * input RDD and the second element is a tuple containing optional value from source RDD and value from
     * input RDD. An optional value from the source RDD is represented with the Option type
     *
     * The leftOuterJoin is similar to the rightOuterJoin. The following example shows how they work
     * A=(a,b,c) B=(b,c,d) A.leftOuterJoin(B)=(a,b,c), A.rightOuterJoin(B)=(b,c,d), A.fullOuterJoin(B)=(a,b,c,d)*/

    val leftOuterJoin=pair1.leftOuterJoin(pair2)
    println(s" Left outer join result of two pairs : ${leftOuterJoin.collect.mkString(",")}")
    val rightOuterJoin=pair1.rightOuterJoin(pair2)
    println(s" Right outer join result of two pairs : ${rightOuterJoin.collect.mkString(",")}")
    val fullOuterJoin=pair1.fullOuterJoin(pair2)
    println(s" full outer join result of two pairs : ${fullOuterJoin.collect.mkString(",")}")
 /* Note that in the result value tuple of leftJoin, the element from source is the origin type, and the element from
  * input rdd has the option type some(T) or None, this because the input rdd may not have the key, to avoid exception
  * we use option type, for the rightJoin, it's the element of source rdd has the option type*/


/********************************SampleByKey *****************************************************/
 /* The sampleByKey method returns a subset of the source RDD sampled by key. It takes the sampling rate for
* each key as input and returns a sample of the source RDD. This method is used for sampling values of many
* duplicate keys*/

    val duplicateKeyPair=spark.sparkContext.parallelize(List(("a", 1), ("b",2), ("a", 11),("b",22),("a", 111), ("b",222),
   ("c", 111), ("c",222)))
    //println(s" duplicate key pair value: ${duplicateKeyPair.collect().mkString(",")}")
    //The sampleFraction map must contain all keys, and the sum of fractions of each key must be 1. otherwise
    // sampleByKey will return empty rdd
    val sampleFractions=Map("a"->0.1,"b"->0.8,"c"->0.1)
    // Get an approximate sample from each stratum
    val samplePair=duplicateKeyPair.sampleByKey(true,sampleFractions)
    println(s"Approximate sample pair of duplicate key pair value: ${samplePair.collect().mkString(",")}")

    // Get an exact sample from each stratum, this will cost more resource
    val exactSamplePair=duplicateKeyPair.sampleByKeyExact(true,sampleFractions)
    println(s"Exact sample pair of duplicate key pair value: ${exactSamplePair.collect().mkString(",")}")

    /************************************** substractByKey ******************************************/
    /* The subtractByKey method takes an RDD of key-value pairs as input and returns an RDD of key-value pairs
     * containing only those keys that exist in the source RDD, but not in the input RDD.
     * A=(a,b,c) B=(b,c,d) A.subtractByKey(B)=(a)
     * */

    val substractPair=pair1.subtractByKey(pair2)
    println(s" substractPair value is ${substractPair.collect().mkString(",")}")

/************************************GroupByKey*************************************************/

 /* The groupByKey method returns an RDD of pairs, where the first element in a pair is a key from the source
  * RDD and the second element is a collection of all the values that have the same key. It is similar to the
  * groupBy method that we saw earlier. The difference is that groupBy is a higher-order method that takes as
  * input a function that returns a key for each element in the source RDD. The groupByKey method operates on
  * an RDD of key-value pairs, so a key generator function is not required as input. Otherwise, we need to give
  * a key to groupBy*/

    val groupDuplicateKeyPair=duplicateKeyPair.groupByKey()
    println(s"Grouped duplicate key pair value: ${groupDuplicateKeyPair.collect.mkString(",")}")

    /* Note that, the return rdd has the form (b,CompactBuffer(2, 22, 222)) */

    /************************************ ReduceByKey ********************************************/
    /* The higher-order reduceByKey method takes an "associative binary operator (which means a op b == b op a)",
     * as input and reduces values with the same key to a single value using the specified binary operator.
     *
     * A binary operator takes two values as input and returns a single value as output. An associative operator
     * returns the same result regardless of the grouping of the operands. e.g (a + b)+c=a+(b+c)
     *
     * The reduceByKey method can be used for aggregating values by key. For example, it can be used for
     * calculating sum, product, minimum or maximum of all the values mapped to the same key.*/

    val sumByKey=duplicateKeyPair.reduceByKey((x,y)=>x+y)
    val minByKey=duplicateKeyPair.reduceByKey((x,y)=> if (x<y) x else y)
    println(s"sumByKey value is ${sumByKey.collect().mkString(",")}")
    println(s"minByKey value is ${minByKey.collect().mkString(",")}")

/* Note that, here reduceByKey is a transfromation, unlike reduce which is an action. Because reduce is
* aggregating/combining all the elements, while reduceByKey defined on RDDs of pairs is aggregating/combining all
* the elements for a specific key thereby its output is a Map<Key, Value> and since it may still be processed with
* other transformations, and still being a potentially large distributed collection, so it's better to let it continue
* to be an RDD[Key,Value], it is optimal from a pipelining perspective. The reduce cannot result in an RDD simply
* because it is a single value as output.*/

    /*******************************************foldByKey ************************************/

    /*
    * We can use foldByKey operation to aggregate values based on keys. Unlike fold(action), foldByKey is an
    * transformation, the reason is similar to reduceByKey, foldByKey only do the fold action of one key, and it
    * returns a new Array of key-pair which may need new transformation, so it's better to keep it as transformation and
    * return an rdd.
    *
    * Fold do the aggregation on all elements of the rdd, and return a single value. So it's an action.
    *
    * In this example, employees are grouped by department name. If you want to find the maximum salaries in a
    * given department we can use following code.*/
    val deptEmployees = List(
      ("cs",("jack",1000.0)),
      ("cs",("bron",1200.0)),
      ("phy",("sam",2200.0)),
      ("phy",("ronaldo",500.0))
    )
    /****makeRDD is identical to parallelize, because makeRDD also calls parallelize*/
    val employeeRDD = spark.sparkContext.makeRDD(deptEmployees)

    /* foldByKey will first group all elements which have the same key in an Array then apply fold on it. So the
    * accumulator is in the form of value only*/
    /* ("dummy",0.0) is the start value of the accumulator*/
    val maxByDept = employeeRDD.foldByKey(("dummy",0.0))((acc,element)=> if(acc._2 > element._2) acc else element)

    // reduceByKey is much easier to use compare to foldByKey
    val reduceMaxByDept=employeeRDD.reduceByKey((x,y)=>if(x._2>y._2) x else y)

    println(s"maximum salaries in each dept ${maxByDept.collect().mkString(",")}" )

    println(s"maximum salaries in each dept calculated by reduceByKey : ${reduceMaxByDept.collect().mkString(",")}" )

    /* Compare to foldByKey, fold does not group element by key, so the accumulator need to have the same structure of
    * the element of the rdd.*/
    val maxOfAllDept = employeeRDD.fold(("dummy",("dummy",0.0)))(
      (acc,employee)=> if (acc._2._2>employee._2._2) acc else employee
    )
    println(s"maximum salaries of all dept ${maxOfAllDept}")
  }


  /*****************************************************************************************
    * ********************************* Paire RDD Actions **************************
    * **************************************************************************************/
  def PaireRDDActionOperation(spark:SparkSession):Unit={
    /****************************** CountByKey **********************************/
    /* The countByKey method counts the occurrences of each unique key in the source RDD. It returns a Map of
     * key-count pairs.*/

    val duplicateKeyPair=spark.sparkContext.parallelize(List(("a", 1), ("b",2), ("a", 11),("b",22),("a", 111), ("b",222),
      ("c", 111), ("c",222)))
    val countOfEachKey=duplicateKeyPair.countByKey()

    println(s"countOfEachKey value is : ${countOfEachKey}")

    /******************************* lookup *************************************/
    /* The lookup method takes a key as input and returns a sequence(WrappedArray) of all the values mapped to that key in the
     * source RDD.*/

    val lookUpAValues=duplicateKeyPair.lookup("a")
    println(s"lookUpAValues has value: ${lookUpAValues}")


  }
}
