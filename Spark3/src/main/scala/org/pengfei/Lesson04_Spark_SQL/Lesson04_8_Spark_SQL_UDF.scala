package org.pengfei.Lesson04_Spark_SQL

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions.{col, lower, regexp_replace, udf}

object Lesson04_8_Spark_SQL_UDF {

  def main(args: Array[String]) = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val spark = SparkSession.builder().master("local[2]").appName("Lesson04_8_Spark_SQL_UDF").getOrCreate()

    /** ************************************4.8 Spark SQL UDF ***********************************************/

    /* Spark let’s you define custom SQL functions called user defined functions (UDFs). UDFs are great when
    * built-in SQL functions are not able to do what you want, but should be avoid when possible. Because they have
    * very poor performance. For more information, please check:
    * https://jaceklaskowski.gitbooks.io/mastering-spark-sql/content/spark-sql-udfs-blackbox.html
    * */

    // udfExp1(spark)

    // This wil raise null pointer exception
    // udfExp2(spark)

    //Udf handles null value
    //udfExp3(spark)

    // Use column function
    udfExp4(spark)
  }

  /** **************************************4.8.1 Define a udf **********************************/

  /* First step: we create a normal scala function*/
  def lowerRemoveAllWhitespace(s: String, replacement: String): String = {
    s.toLowerCase().replaceAll("\\s", replacement)
  }

  /* Second step: we register the above scala function as a spark UDF by using function spark.sql.functions.udf
  *
  * Note that the udf function take a list of argument type. The first type is the type of the lowerRemoveAllWhitespace
  * return value. The second and third type are the type of the lowerRemoveAllWhitespace argument.
  *
  * */
  val lowerRemoveAllWhitespaceUDF = udf[String, String, String](lowerRemoveAllWhitespace)

  /** **********************************4.8.2 Use a udf ***************************************/

  /* Run the udfExp1 to see how the udf acts on the data frame*/
  def udfExp1(spark: SparkSession) = {
    import spark.implicits._
    val sourceDF = Seq(
      ("  HI THERE     ", "_"),
      (" GivE mE PresenTS     ", "")
    ).toDF("val", "replacement")


    sourceDF.select(
      lowerRemoveAllWhitespaceUDF(col("val"), col("replacement")).as("clean_val")
    ).show()
  }

  /** ******************************** 4.8.3 Handle the null value in a udf **********************/

  /* If we have null value in the sourceDF, our udf will raise errors. Because our udf does not handle the null value
  * at all. So now we have to add logic to handle the null value. But we don't want to do that.
  *
  * We can use the option type to handle the null value, thanks to Scala.
  * */
  def udfExp2(spark: SparkSession) = {
    import spark.implicits._
    val sourceDF = Seq(
      ("  HI THERE     ", "_"),
      (" GivE mE PresenTS     ", ""),
      (null, "")
    ).toDF("val", "replacement")


    sourceDF.select(
      lowerRemoveAllWhitespaceUDF(col("val"), col("replacement")).as("clean_val")
    ).show()
  }

  /* Here we write a new udf which handles the null value.
  * First we transform the two string argument to Option[String]
  * Second, we transform the return string value to Option[String]*/
  def betterLowerRemoveAllWhitespace(s: String, replacement: String): Option[String] = {
    val str = Option(s).getOrElse(return None)
    val rep = Option(replacement).getOrElse(return None)
    Some(str.toLowerCase().replaceAll("\\s", rep))
  }

  val betterLowerRemoveAllWhitespaceUDF = udf[Option[String], String, String](betterLowerRemoveAllWhitespace)

  def udfExp3(spark: SparkSession) = {
    import spark.implicits._
    val sourceDF = Seq(
      ("  HI THERE     ", "_"),
      (" GivE mE PresenTS     ", ""),
      (null, "")
    ).toDF("val", "replacement")


    sourceDF.select(
      betterLowerRemoveAllWhitespaceUDF(col("val"), col("replacement")).as("clean_val")
    ).show()

    // Explain the physical plan of the udf
    sourceDF.select(
      betterLowerRemoveAllWhitespaceUDF(col("val"), col("replacement")).as("clean_val")
    ).explain()

  }

  /** ******************************** 4.8.4 Use column function to replace UDF **********************/

  /*
  * As we mentioned early, UDF is not optimal at all. So use UDF as less as you can. In this section, we show you how to
  * define column function which can apply directly to a data frame without creating UDF.
  *
  * Note that, lower and regexp_replace are built-in functions in spark.sql.functions.
  * */

  def bestLowerRemoveAllWhitespace(col: Column): Column = {
    lower(regexp_replace(col, "\\s+", ""))
  }

  def udfExp4(spark: SparkSession) = {
    import spark.implicits._
    val sourceDF = Seq(
      ("  HI THERE     ", "_"),
      (" GivE mE PresenTS     ", ""),
      (null, "")
    ).toDF("val", "replacement")


    sourceDF.select(
      bestLowerRemoveAllWhitespace(col("val")).as("clean_val")
    ).show()

    // Explain the physical plan
    sourceDF.select(
      bestLowerRemoveAllWhitespace(col("val")).as("clean_val")
    ).explain()

  }
}
