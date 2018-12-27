package org.pengfei.Lesson17_Analyze_Clinical_Data

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions._

object Lesson17_Row_To_Column_To_Row {

  /* implicit class NullOccludingMap[K, V](private val underlying: Map[K, V]) extends AnyVal {
    def getNonNullOrElse(key: K, default: V): V = {
      underlying.get(key) match {
        case Some(value) if value != null => value
        case _ => default
      }
    }
  }*/

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val spark = SparkSession.builder().master("local[2]").appName("Lesson10_Spark_Application_ETL").getOrCreate()
    import spark.implicits._

    /* Here, we reuse the data from Lesson10, in Lesson10 we generate stats of the data with the code below,
    * to accelarate, we output the stats in a file named stats.csv. We just read it directly not generate each time
     * It can be generate with the following code*/
    /*  val filePath="/DATA/data_set/spark/basics/Lesson10_Spark_Application_ETL/hospital_data"
    val outputPath="/DATA/data_set/spark/basics/Lesson17_Analyse_Clinical_Data"
    val block1Name="/block_1.csv"

    val df=spark.read.option("header","true").option("nullValue","?").option("inferSchema","true").csv(filePath+block1Name)
    val stats=df.describe()
      stats.show(5)

    stats.coalesce(1).write.mode(SaveMode.Overwrite)
      .option("header","true")
      .option("encoding","UTF-8")
      .option("mapreduce.fileoutputcommitter.marksuccessfuljobs","false") //Avoid creating of job success files
      .csv(outputPath+"/row_to_Column")*/

    val statsFilePath = "/DATA/data_set/spark/basics/Lesson17_Analyse_Clinical_Data/row_to_Column/stats.csv"
    val stats = spark.read.option("header", "true").option("nullValue", "?").option("inferSchema", "true").csv(statsFilePath)
    stats.show(5)
    //stats.printSchema()
    /** ************************************************************************************************************
      * *******************************1. Column to Row *********************************************************
      * ************************************************************************************************************/
    /* In the first part we want to transform colulmn to row,

    origin df (stats)
      |summary|              id_1|             id_2|      cmp_fname_c1|      cmp_fname_c2|       cmp_lname_c1|   ...  |
      +-------+------------------+-----------------+------------------+------------------+-------------------+
      |  count|            574913|           574913|            574811|             10325|             574913|
      |   mean|33271.962171667714| 66564.6636865056|0.7127592938252765|0.8977586763518972|0.31557245780987964|
      | stddev| 23622.66942593358|23642.00230967225|0.3889286452463553|0.2742577520430534| 0.3342494687554251|
      |    min|                 1|                6|               0.0|               0.0|                0.0|
      |    max|             99894|           100000|               1.0|               1.0|                1.0|
      +-------+------------------+-----------------+------------------+------------------+
    *
    * to the following form statsInRow
*
* +------+------------+-------------------+
|metric| field| value|
+------+------------+-------------------+
| count| id_1| 5749132.0|
| count| id_2| 5749132.0|
| count|cmp_fname_c1| 5748125.0|
...
| count| cmp_by| 5748337.0|
| count| cmp_plz| 5736289.0|
| mean| id_1| 33324.48559643438|
| mean| id_2| 66587.43558331935|*/
    val columns = stats.columns
    val statsInRow: Dataset[(String, String, Double)] = stats.flatMap(row => {
      // The first column is the metric id, which is also the first column of origin df
      val metric = row.getAs[String](0)

      // The second column is the filed name, which is the column name of origin df
      // The third column is the filed value, which is the row value of each column of the origin df.
      // we have 11 column in one row of the origin df which need to transform into 11 rows in the new df
      (1 until row.size).map(i =>
        (metric, columns(i), row.getDouble(i))
      )

    })

    val statsInRowDf = statsInRow.toDF("metric", "filedID", "filedValue")
    statsInRowDf.show(5)

    /** ************************************************************************************************************
      * *******************************1. Row to Column *********************************************************
      * ************************************************************************************************************/

    /* Row to Column has two scenarios:
  * - 1. row number for each subject is the same and the order of the filed is the same (In this example), We can use
  *      pivot function to the job easily
  * - 2. row number for each subject is different, we can not use the pivot function anymore.
   *
   * In this Lesson, we will show how to work with the two scenarios*/

    /** *********************************** Scenario 1 **************************************************************/
    val statsInColumn = statsInRowDf.groupBy("metric")
      .pivot("filedID")
      .agg(expr("coalesce(first(filedValue), \"pending\")"))

    statsInColumn.show(10)

    /** ***********************************Scenario 2 ***********************************************************/
    /* Pivot won't work in Scenario 2, because the number and order of rows for different subjects are not the same,
    * Solution 1, add null rows with right order for each subjects, too complex to do
    * Solution 2, Step1. get all possible filedName,
    *             Step2. convert rows of the same subject(metric) to a column of a list of (key,value) pair,
    *                    if the filedValue row does not exist for the given filedName, pull null as value.
    *             Step3. For each (key,value), create a column*/

val statsInColumn2=RowToColumn(statsInRowDf,"metric","filedID","filedValue")
    statsInColumn2.show(10)

  }

  def RowToColumn(df: DataFrame, objectIdColumnName:String,targetIdColumnName: String,targetValueColumnName:String): DataFrame = {
    val spark = df.sparkSession
    import spark.implicits._

    /* Step1. Get all filedIDs,*/
    val filedIDs = df.select(targetIdColumnName).distinct().orderBy(df(targetIdColumnName).asc)

    //filedIDs.show(10)

    // make the Array of the filed Ids.
    val filedIDsArray: Array[String] = filedIDs.collect().map(_.get(0).toString)


    /*for(filedId<-filedIDsArray){
      println(s"filedId value is ${filedId}")
    }
*/
    /* Step2. Build the <key,value> map for each subject.  */


    /* //Spark provide map function which do the same thing as the following code
    def buildFiledMap(filedName:String,filedValue:Double):Map[String,Double]={
      Map(filedName->filedValue)
    }
    spark.udf.register("buildFiledMap",(arg1:String,arg2:Double)=>buildFiledMap(arg1,arg2))
    val statsMap=statsInRowDf.withColumn("filed_map",expr("buildFiledMap(filedID,filedValue)"))
    statsMap.show(5)
    */

    val filedIdValueMap = df.withColumn("filed_map", map(df(targetIdColumnName), df(targetValueColumnName)))

    //filedIdValueMap.show(5)

    /* Step3. Group the filed map for each distinct subject */
    val groupedStats = filedIdValueMap.groupBy(objectIdColumnName)
      .agg(collect_list("filed_map"))
      .as[(String, Seq[Map[String, Double]])] // <-- leave Rows for typed pairs
      .map { case (id, list) => (id, list.reduce(_ ++ _)) } // <-- collect all entries under one map
      .toDF(objectIdColumnName, "filed_map")

   // groupedStats.show(10, false)

    /* Step 4. Complete filed map for missing filed*/
    val bFiledIDsArray: Broadcast[Array[String]] = spark.sparkContext.broadcast(filedIDsArray)

    /*val completeStats = groupedStats.map(row => {
      val metric = row.getString(0)
      var currentFiledMap = scala.collection.mutable.Map(row.getAs[Map[String, Double]](1).toSeq: _*)
      (0 until bFiledIDsArray.value.length).map { i =>
        val filedId: String = bFiledIDsArray.value(i)

        if (!currentFiledMap.contains(filedId)) {
          currentFiledMap += (filedId -> 12345.54321)
        }
      }
      (metric, currentFiledMap)
    })
    val completeStatsDf = groupedStats.toDF("metric", "filed_map")
    completeStatsDf.show(10, false)
    */



    /* Step 5. Create column for each field, with the getFiledValue function, the step 4 may be omitted*/

    def getFiledValue(filedId: String, filedMap: Map[String, Double]): String = {
      filedMap.getOrElse(filedId, "").toString
    }

    spark.udf.register("getFiledValue", (arg1: String, arg2: Map[String, Double]) => getFiledValue(arg1, arg2))
    var tmpDf = groupedStats
    (0 until bFiledIDsArray.value.length).map { i =>
      val filedId: String = bFiledIDsArray.value(i)
      tmpDf = tmpDf.withColumn("current_id", lit(filedId))
        .withColumn(filedId, expr("getFiledValue(current_id,filed_map)"))
        .drop("current_id")
    }

    val result=tmpDf.drop("filed_map")
    return result
  }


}

