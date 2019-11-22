package org.pengfei.Lesson04_Spark_SQL

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Lesson04_Exc04_Parse_Apache_Access_Log {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val spark = SparkSession.builder().master("local[2]").appName("Lesson4_Exec04_Parse_Apache_Access_Log").getOrCreate()

    import spark.implicits._
    val sparkConfig = ConfigFactory.load("application.conf").getConfig("spark")
    val path = sparkConfig.getString("sourceDataPath")
    val filePath = s"${path}/spark_lessons/Lesson04_Spark_SQL/access.log.2"
    // Read raw data
    val rawDf = spark.read.text(filePath)

    rawDf.cache()
    rawDf.show(5, false)
    rawDf.count()

    // split raw data into token list
    val splitDf = rawDf.select(split(col("value"), " ").as("tokenList"))
    splitDf.show(5, false)

    // transform token list into dataframe
    val tokenizedDf = splitDf.withColumn("host", $"tokenList".getItem(0))
      .withColumn("rfc931", $"tokenList".getItem(1))
      .withColumn("authuser", $"tokenList".getItem(2))
      .withColumn("date", concat($"tokenList".getItem(3), $"tokenList".getItem(4)))
      .withColumn("request", $"tokenList".getItem(6))
      .withColumn("status", $"tokenList".getItem(8))
      .withColumn("bytes", $"tokenList".getItem(9))
      .drop("tokenList")

    tokenizedDf.show(5, false)

    tokenizedDf.select("status").distinct()

    /* The following request give us the top ten most visited page. We could noticed that the second most viewed item is not in the top 10 sell list */
    val mostViewedPage = tokenizedDf.filter($"request".contains("product")).groupBy($"request").count().orderBy($"count".desc)

    mostViewedPage.show(10, false)

    /* If we want to replace the 20% by space in the request, we can use the regexp_replace*/
    val betterView = mostViewedPage.select(regexp_replace($"request", "%20", " ").alias("request"), $"count")
    betterView.show(10, false)

    /* refine data frame, only keep product name, and rename column name*/

    /*Here we use a interesting spark sql function substring_index to get the product name
    * substring_index(str, delim, count) : Returns the substring from str before count occurrences of the delimiter
    *          delim. If count is positive, everything to the left of the final delimiter (counting from the left) is
    *          returned. If count is negative, everything to the right of the final delimiter (counting from the right)
    *          is returned.
    *
    * For example, if we want to keep the head of the string(www), then we do the following
    * SELECT substring_index('www.apache.org', '.', 1);
    * If we want to keep the tail of the string(org), then we do the following
    * SELECT substring_index('www.apache.org', '.', -1);
    * */

    /* After analysis, we found we have false data in access log, so we want to remove all lines which has "add_to_cart"
    * as product_name, we use filter() which takes boolean expression as argument, notice we can't use ! for negation
    * here, we need to use not()
    * */
    val productVisitNumber = betterView
        .withColumn("product_name",substring_index(col("request"),"/",-1))
        .withColumnRenamed("count","view_number")
        .drop("request")
        .filter(not($"product_name".contains("add_to_cart")))
        .select("product_name","view_number")

    productVisitNumber.show(10,false)

  }


}
