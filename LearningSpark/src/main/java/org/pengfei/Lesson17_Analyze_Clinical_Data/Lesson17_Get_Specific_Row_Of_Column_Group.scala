package org.pengfei.Lesson17_Analyze_Clinical_Data

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object Lesson17_Get_Specific_Row_Of_Column_Group {

  /* In this section, we will show you how to get specific rows of each groups after a groupby. We suppose */
  def main(args:Array[String]):Unit={
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val spark = SparkSession.builder().appName("Lesson17_Appendix").master("local[2]").getOrCreate()
    import spark.implicits._

    val df = spark.sparkContext.parallelize(Seq(
      (0,"cat26",30.9), (0,"cat13",22.1), (0,"cat95",19.6), (0,"cat105",1.3),
      (1,"cat67",28.5), (1,"cat4",26.8), (1,"cat13",12.6), (1,"cat23",5.3),
      (2,"cat56",39.6), (2,"cat40",29.7), (2,"cat187",27.9), (2,"cat68",9.8),
      (3,"cat8",35.6))).toDF("Hour", "Category", "TotalValue")

    df.orderBy($"TotalValue".desc).show(10)
     // we can't do orderBy or sortBy after groupBy, if we do orderBy and sortBy before, the groupBy will
    // not preserve the order, so the following code is wrong, sometimes it may give you right answer
    // but there is no guarantee.
    val maxValueOfEachHour=df.orderBy($"TotalValue".desc).groupBy($"Hour").agg(first("Hour").as("Hour"),
       first("Category").as("Category"),
       first("TotalValue").as("TotalValue"))

    maxValueOfEachHour.orderBy($"Hour".desc).show(5)

    /* Solution 1 : Use window function
    *
    * 1. Create window specification partitionBy hour and orderby TotalValue as desc, so the first row is the biggest
    * 2. use function row_numb to create a new column row_number
    * 3. filter the row with row_number=1
    * */

   /* val w=Window.partitionBy($"Hour").orderBy($"TotalValue".desc)

    val windowSolution = df.withColumn("rn", row_number.over(w)).where($"rn" === 1)
    windowSolution.show(5)*/

    /* Solution 2 : Use dataSet Api,
    # In the following code, we use groupByKey to group row(Record) by using Record attributes
    * Hour, then we use reduceGroups to reduce all elements in the same group after groupBy.
    * The reduceGroups takes a list of Record of the same group, and return one Record.
    *
    * This method can leverage map side combine and don't require full shuffle so most of the time should exhibit a
    * better performance compared to window functions and joins. These cane be also used with Structured Streaming
    * in completed output mode.
    * */
  /*  val goodSolution=df.as[Record].groupByKey(_.Hour).reduceGroups((x,y)=>if (x.TotalValue > y.TotalValue) x else y)
     goodSolution.show(5)*/

    /* Solution 3 : Use sql aggregation and join
    *
    * 1. We do a groupBy over column hour, then aggregate on column TotalValue with max to get the max value of each hour
    * 2. Then we join with original on which hour==max_hour && TotalValue=max_value*/
    val dfMax = df.groupBy($"hour".as("max_hour")).agg(max($"TotalValue").as("max_value"))
    //dfMax.show(5)

    val joinSolution=df.join(broadcast(dfMax),($"hour"===$"max_hour")&&($"TotalValue"===$"max_value"))
      .drop("max_hour")
      .drop("max_value")

    joinSolution.show(5)

  }

  case class Record(Hour: Integer, Category: String, TotalValue: Double)
}
