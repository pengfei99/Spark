package org.pengfei.Lesson05_Spark_ML

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._

object Lesson05_6_Spark_Extra {

  def main(args:Array[String])={
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val spark = SparkSession.builder().master("local[2]").appName("Lesson5_6_Spark_Extra").getOrCreate()

/**********************************5.6.1 Dealing with null in Spark *****************************************/
    SparkNullExample(spark)
  }
/*********************************************************************************************
  * ******************************5.6.1 Dealing with null in Spark ****************************
  * *******************************************************************************************/

  /* Datasets are usally filled with many null values and you’ll constantly need to write code that
  * gracefully handles these null values. You’ll need to use null values correctly in spark, adhere
  * to Scala best practices regarding null values, and possibly break Scala best practices for performance
  * sensitive code.
  *
  * In this section, we will explain how to work with null in Spark
  *
  * What is null?
  *
  * In SQL databases, "null" means that some value is unknown, missing, or irrelevant." The SQL concept of null is
  * different than null in programming languages like scala. Spark dataframe best practices are aligned with sql best
  * practices, so Spark should use null for values that are unknow, missing or irrelevant.
  */
/*******************************************Null vs NaN ***************************************/

  /* null values represents "no value" or "nothing", it's not even an empty string or zero. It can be used to
   * represent that nothing useful exists.
   *
   * NaN stands for "Not a Number", it's usually the result of a mathematical operation that doesn't
   * make sense, e.g. 0.0/0.0.*/

  /****************************** Spark support null and NaN value **********************************/
  /* name,country,zip_code
      joe,usa,89013
      ravi,india,
       "",,12389
       "",,
       NaN,,NaN

   *
   * When spark read the above csv file into a dataframe, all the blank values and empty strings are read into a null
   * by the spark sql lib (since spark 2.0.1)
   *
   * In spark dataset/frame, all null value of data source is consider unknown or missing.
   * See the below example*/

  def SparkNullExample(spark:SparkSession):Unit={
    import spark.implicits._
   val filePath="/home/pliu/data_set/spark_data_set/spark_lessons/Lesson5_Spark_ML/sample_null_value.csv"
    val df=spark.read.option("header","true").option("inferSchema","true").csv(filePath)
    df.show()

    /* Spark function may also return null value, for example, we try to calculate the mean and stddev of a
    * string column, it returns null, because the return value is irrelevant.*/

    val letterDf= Seq(("a"),("b")).toDF("letter")
    letterDf.describe().show()

    /* Other example, when joining DataFrames, the join column will return null when a match cannot be made. */


/**************************************Get null/na count***********************************/
    /* we  can use isNull or isNan, isNull can only detect the null/empty value, isNaN can detect only NaN */

    val nullInName=df.filter($"name".isNull).count()
    val nullInCountry=df.filter($"country".isNull).count()
    val nullInCode=df.filter($"zip_code".isNull).count()

    println(s"null value number in Name $nullInName, in country $nullInCountry, in zip_code $nullInCode")

    val nanInName=df.filter($"name".isNaN).count()
    val nanInCountry=df.filter($"country".isNaN).count()
    val nanInCode=df.filter($"zip_code".isNaN).count()

    println(s"nan value number in Name $nanInName, in country $nanInCountry, in zip_code $nanInCode")

    val totalInName=df.filter($"name".isNull||$"name".isNaN).count()
    val totalInCountry=df.filter($"country".isNull||$"country".isNaN).count()
    val totalInCode=df.filter($"zip_code".isNull||$"zip_code".isNaN).count()

    println(s"Total null value number in Name $totalInName, in country $totalInCountry, in zip_code $totalInCode")

    /**********************************Scala null Conventions********************************/
    /* David Pollak, the author of Beginning Scala, stated “Ban null from any of your code. Period.”
    * Alvin Alexander, a prominent Scala blogger and author, explains why Option is better than null in
    * this blog post. The Scala community clearly perfers Option to avoid the pesky null pointer exceptions
    * that have burned them in Java.
    *
    * But spark sql is more sql than scala. And for performance sensitive code, null is better than option,
    * in order to avoid virtual method calls and boxing.*/

    /*************************************nullable Columns*************************************/
    /*When we define a dataset/frame schema, we could specify the value of a column is nullable or not. For
    * example, name column is not nullable, and age column is nullable. In other words, the name column
    * cannot take null values, but the age column can take null values.*/
    val schema= List(
      StructField("name",StringType,false),
      StructField("age",IntegerType,true)
    )
    val data=Seq(
      Row("miguel",null),
      Row("luisa",21)
    )
    val df1=spark.createDataFrame(spark.sparkContext.parallelize(data),StructType(schema))
    df1.show()
    /* If we set one of the name rows to be null, the code will blow up with this error:
    * “Error while encoding: java.lang.RuntimeException: The 0th field ‘name’ of input row
    * cannot be null”.*/

    /*******************************Transform column which contains null values***********************/
    val numRDD=spark.sparkContext.parallelize(List(
      Row(1),
      Row(8),
      Row(12),
      Row(null)
    ))
    // println(numRDD.toDebugString)
    // println(numRDD.collect().toArray.mkString(";"))
    val numSchema= List(
      StructField("number",IntegerType,true)
    )
    val numDf= spark.createDataFrame(numRDD,StructType(numSchema))
    numDf.show()
    /* Now we want to add a new column which tells if the number is even or not, first we use function isEvenSimple
    * which does not consider the num value is null*/
    //val isEvenSimpleUdf=udf[Boolean,Integer](isEvenSimple)
    spark.udf.register("isEvenSimple",(number:Int)=>isEvenSimple(number))
    spark.udf.register("isEvenBad",(num:Int)=>isEvenBad(num))
    spark.udf.register("isEvenScala",(num:Int)=>isEvenScala(num))
    val evenSimpleDf=numDf.withColumn("is_even",expr("isEvenSimple(number)"))
    evenSimpleDf.show()

    // here we use a when(condition,value) method, we only call isEvenSimple function when the number is not null
    // ifnot we just fill null value
    val evenGoodDf=numDf.withColumn("is_even_Good",when(col("number").isNotNull,expr("isEvenSimple(number)")).otherwise(lit(null)))
     evenGoodDf.show()

    val evenBadDf=numDf.withColumn("is_even_bad",expr("isEvenBad(number)"))
    evenBadDf.show()

    val evenRBadDf=numDf.withColumn("is_even_Rbad",when(col("number").isNull,false).otherwise(expr("isEvenBad(number)")))
    evenRBadDf.show()

    val evenScalaDf=numDf.withColumn("is_even_Scala",expr("isEvenScala(number)"))
    evenScalaDf.show()

    // With all the above test, finally spark manage well null value transformation, so it's enough to just
    // use the isEvenSimple function

    /***************************************fill null/nan with other value************************************/
    /*To deal with null or nan, there is only two ways:
    * - drop the row with null/nan value
    * - fill the null cell with a value, In general it's the mean of the column,
    * In spark we can use .na.drop or na.fill to do these tasks
    * na.drop will drop any rows which contains null or nan
    * na.fill(arg) respect the type of the column, for example na.fill(0) will repalce all column of type int
    * na.fill("Na") will replace all column with type string*/
val afterDropDf=df.na.drop()
    afterDropDf.show()
    val afterFillIntDf=df.na.fill(0)
    afterFillIntDf.show()
    val afterFillStrDf=df.na.fill("NA")
    afterFillStrDf.show()
/*
*You can notice that the name column still has NaN, because only in columns of digit type NaN means "Not a Number".
*In string column NaN is considered as a normal value.
*/


  }
def isEvenSimple(num:Int):Boolean={
  num%2 ==0
}
  // this function treat null badly, it may confuse data engineer with a false value in the is_even column and the number
  // is null.
  def isEvenBad(num:Int):Boolean={
    // we can't compare int with null. so we can never enter in the if, it always go to the else
    if(num==null){
      false
    }else{
      num%2==0
    }
  }

  /*Best way to deal with null in scala is to use option type*/
  def isEvenScala(num:Int):Option[Boolean]={
    if(num==null){
      None
    }
    else{
      Some(num%2==0)
    }
  }
}
