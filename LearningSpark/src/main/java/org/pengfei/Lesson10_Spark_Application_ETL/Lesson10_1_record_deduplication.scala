package org.pengfei.Lesson10_Spark_Application_ETL

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Dataset

object Lesson10_1_record_deduplication {
  /*******************************************************************************************************
    *******************************10.1 Record deduplication***********************************************
    *******************************************************************************************************/

  /********************************10.1.1 Introduction*********************************************/
  /* The problem that we’re going to study in this chapter goes by a lot of different names
   * in the literature and in practice: record linkage, entity resolution, record deduplication, merge-and-purge,
   * and list washing.
   *
   * The general structure of the problem is something like this: we have a large collection
   * of records from one or more source systems, and it is likely that multiple records
   * refer to the same underlying entity, such as a customer, a patient, or the location of a
   * business or an event. Each entity has a number of attributes, such as a name, an
   * address, or a birthday, and we will need to use these attributes to find the records that
   * refer to the same entity. Unfortunately, the values of these attributes aren’t perfect:
   * values might have different formatting, typos, or missing information that means that
   * a simple equality test on the values of the attributes will cause us to miss a significant
   * number of duplicate records.
   *
   * For example
   * Name            | Address               | City           |   State |  Phone
* Josh’s Coffee Shop | 1234 Sunset Boulevard | West Hollywood | CA      | (213)-555-1212
* Josh Coffee        | 1234 Sunset Blvd West | Hollywood      | CA      | 555-1212
* Coffee Chain #1234 | 1400 Sunset Blvd #2   | Hollywood      | CA      | 206-555-1212
* Coffee Chain Regional Office | 1400 Sunset Blvd Suite 2 | Hollywood | California | 206-555-1212
*
* The first two entries in this table refer to the same small coffee shop, even though a
* data entry error makes it look as if they are in two different cities (West Hollywood
* and Hollywood). The second two entries, on the other hand, are actually referring to
* different business locations of the same chain of coffee shops that happen to share a
* common address: one of the entries refers to an actual coffee shop, and the other one
* refers to a local corporate office location. Both of the entries give the official phone
* number of corporate headquarters in Seattle.
* */

  /********************************10.1.2 sample Dataset*********************************************/

  /* In this lesson, we will use a sample data from UC irvine Machine Learning repository.
  * The data set we’ll analyze was curated from a record linkage study performed
  * at a German hospital in 2010, and it contains several million pairs of patient
  * records that were matched according to several different criteria, such as the patient’s
  * name (first and last), address, and birthday. Each matching field was assigned a
  * numerical score from 0.0 to 1.0 based on how similar the strings were, and the data
  * was then hand-labeled to identify which pairs represented the same person and
  * which did not.
  *
  * Each row represent a match of two patient record, id_1 is the id for patient 1, id_2 is the id for patient 2
  *
  * The underlying values of the fields that were used to create the data set
  * were removed to protect the privacy of the patients. Numerical identifiers, the match
  * scores for the fields, and the label for each pair (match versus nonmatch) were published
  * for use in record deduplication research.
  *
  * $ mkdir linkage
  * $ cd linkage/
  * $ curl -L -o donation.zip https://bit.ly/1Aoywaq
  * $ unzip donation.zip
  * $ unzip 'block_*.zip'*/

  def main(args:Array[String]):Unit={
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val spark=SparkSession.builder().master("local[2]").appName("Lesson10_Spark_Application_ETL").getOrCreate()

    val filePath="/DATA/data_set/spark/basics/Lesson10_Spark_Application_ETL/hospital_data"
    val block1Name="/block_1.csv"

    val block1Df=spark.read.option("header","true").option("nullValue","?").option("inferSchema","true").csv(filePath+block1Name)

    /********************************10.1.3 understand Dataset*********************************************/

    //UnderstandDFOperation(spark,block1Df)

    /* With the above command, we know the schema, the size of the dataset, Now we need to understand each column
    * - The first two fields (id_1, id_2)are integer IDs that represent the patients that were matched in the record
    * - The next nine values are (possibly missing) numeric values (either doubles or ints) that represent match
    *   scores on different fields of the patient records, such as their names, birthdays, and locations. The fields
    *   are stored as integers when the only possible values are match (1) or no-match (0), and doubles
    *   whenever partial matches are possible.
    * - The last field is a boolean value (true or false) indicating whether or not the pair of patient records
    *   represented by the line was a match.
    *
    * The goal of this lesson is to come up with a simple clssifier that allow us to predict whether a record will
    * be a match based on the values of the match scores.
    *
    * */

    /********************************10.1.4 basic statistics of Dataset*********************************************/

  // BasicStatsExample(spark,block1Df)

    /********************************10.1.5 Pivoting and Reshaping of Dataset*********************************************/

    /* 1. For example, we want to transform the summary dataframe
    *
    * |summary|              id_1|             id_2|      cmp_fname_c1|      cmp_fname_c2|       cmp_lname_c1|   ...  |
      +-------+------------------+-----------------+------------------+------------------+-------------------+
      |  count|            574913|           574913|            574811|             10325|             574913|
      |   mean|33271.962171667714| 66564.6636865056|0.7127592938252765|0.8977586763518972|0.31557245780987964|
      | stddev| 23622.66942593358|23642.00230967225|0.3889286452463553|0.2742577520430534| 0.3342494687554251|
      |    min|                 1|                6|               0.0|               0.0|                0.0|
      |    max|             99894|           100000|               1.0|               1.0|                1.0|
      +-------+------------------+-----------------+------------------+------------------+
    *
    * to the following form
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
| mean| id_2| 66587.43558331935|
...
*
* To resolve this kind of transformation, you must start your process from the input dataframe, if I can process the
* dataframe by iterating each row, or some columns.
*
* In our example, we know row 1 has all count value of each filed,  each value's potions correspond a filed name
* e.g. row(1)'s value is for id_1.
* row of dataframe is an array of objects.
* 2. After the transformation to longForm we want to see the count, mean for each field
*/

   // PivotingDataFrameExample(block1Df)

    /**********************10.1.6 Selecting features ****************************************/
/*
* A good feature for classification problem has two properties:
* - The good feature tends to have significantly different values for different label (in our case, it's matches or
*   nonmatches). So the difference between the means of the field of different label will be large
*
* - The good features must occur often enough in the data that we can rely on it to be regularly available for
*   any pair of records.
*
*/

//    SelectingFeaturesOperations(block1Df)
    /* The below stats shows the total which is the total count of the field in both match and unmatch dataset
    * delta is the difference between mean of the match and unmatch.
+------------+--------+-------------------+
|       field|   total|              delta|
+------------+--------+-------------------+
|     cmp_plz|573618.0| 0.9524975516429005|
|cmp_lname_c2|   239.0| 0.8136949970410104|
|      cmp_by|574851.0| 0.7763379425859384|
|      cmp_bd|574851.0| 0.7732820129086737|
|cmp_lname_c1|574913.0| 0.6844795197263095|
|      cmp_bm|574851.0|  0.510834819548174|
|cmp_fname_c1|574811.0| 0.2853115682852544|
|cmp_fname_c2| 10325.0|0.09900440489032625|
|     cmp_sex|574913.0|0.03452211590529575|
+------------+--------+-------------------+
*   cmp_fname_c2 isn’t very useful because it’s missing a lot of the time and the difference in the
*   mean value for matches and nonmatches is relatively small—0.09, for a score that ranges from 0 to 1.
*   The cmp_sex feature also isn’t particularly helpful because even though it’s available for any pair of records, the
*   difference in means is just 0.03.
*
*   Features cmp_plz and cmp_by, on the other hand, are excellent. They almost always occur for any pair of records,
*   and there is a very large difference in the mean values (more than 0.77 for both features.) Features cmp_bd,
*   cmp_lname_c1, and cmp_bm also seem beneficial: they are generally available in the data set and the difference,
*   in mean values for matches and nonmatches are substantial.
*
*   Features cmp_fname_c1 and cmp_lname_c2 are more of a mixed bag: cmp_fname_c1 doesn’t discriminate all that well
*   (the difference in the means is only 0.28) even though it’s usually available for a pair of records, whereas
*   cmp_lname_c2 has a large difference in the means but it’s almost always missing. It’s not quite obvious under
*   what circumstances we should include these features in our model based on this data.
*
 *  For now, we’re going to use a simple scoring model that ranks the similarity of pairs of records based on the
 *  sums of the values of the obviously good features: cmp_plz, cmp_by, cmp_bd, cmp_lname_c1, and cmp_bm. For the
 *  few records where the values of these features are missing, we’ll use 0 in place of the null value in our sum.
 *  We can get a rough feel for the performance of our simple model by creating a data frame of the computed scores
 *  and the value of the is_match column and evaluating how well the score discriminates between matches and
 *  nonmatches at various thresholds.*/

    /********************** 10.1.7 Preparing models for production environments *************************************/
    ProductionExample(block1Df)
  }

  def UnderstandDFOperation(spark:SparkSession,df:DataFrame):Unit={
    df.printSchema()
    val rowNum=df.count()
    val columnNum=df.columns.length
    println(s"Block1 has ${rowNum} rows and ${columnNum} columns")
    df.show(5)
  }

  /********************************10.1.4 basic statistics of Dataset*********************************************/

  def BasicStatsExample(spark:SparkSession,df:DataFrame):Unit={
    // count match and unmatch number
    df.groupBy("is_match").count().orderBy(desc("count")).show()
    // get avg and stddev of cmp_sex
    df.agg(avg("cmp_sex"), stddev("cmp_sex")).show()

    // summary statiscs
    val summary= df.describe()
    summary.show()
    val matched=df.where("is_match=true")
    matched.describe()

    val unmatched=df.filter(df("is_match")===false)
    unmatched.describe()
  }

  /********************************10.1.5 Pivoting and Reshaping of Dataset*********************************************/
  def PivotingDataFrameExample(df:DataFrame):Unit={
    //we can get the sparkSession from the datafram
    import df.sparkSession.implicits._
    //get summary stats
    val summary= df.describe()
    // get the sechma of df which has type StructType(StructField(id_1,IntegerType,true), StructField*)
    val schema = df.schema
    //println(schema.toString())

    val longForm:Dataset[(String,String,Double)]= summary.flatMap(row=>{
      // row.getString(0) get the first element of the current row which is the metric name (e.g. count, mean)
      val metric=row.getString(0)
      // we loop over the rest of the elements in the current row
      (1 until row.size).map(i=> {
        (metric,schema(i).name,row.getString(i).toDouble)
        //schema(i).name returns the name of the column, row.getString(i).toDouble returns the value of the column of
        // the current row
      })
    })

    val longDF=longForm.toDF("metric","field","value")
    longDF.show()

    /****************Now, we want to see the mean, count of each value*********/
    val wideDF=longDF.groupBy("field").pivot("metric",Seq("count","mean","stddev","min","max"))
                 .agg(first("value"))

    /*The pivot operator needs to know the distinct set of values of the pivot column that we want to use for
    * the columns, and we can specify the value in each cell of the wide table by using an agg(first) operation
    * on the values column, which works correctly because there is only a single value for each combination
    * of field and metric*/
    wideDF.show()
  }

  def SelectingFeaturesOperations(df:DataFrame):Unit={
    val spark=df.sparkSession
    val matchSummary=df.filter(df("is_match")===true).describe()
    val missSummary=df.where("is_match=false").describe()
/*matchSummary.show(5)
    missSummary.show(5)*/
    // use the method pivotSummary in Pivot.scala to transform the match and miss dataset
    val matchST=Pivot.pivotSummary(matchSummary)
    val missST=Pivot.pivotSummary(missSummary)

    //matchST.show(5)
    //missST.show(5)

    /* We can use isin function to execulde the field id_1, id_2, because we know it's not a feature
    * We select only field , count, and mean column*/
    val execuldeList=List("id_2","id_1")
    val cleanMatch=matchST.filter(!col("field").isin(execuldeList:_*)).select("field","count","mean")
    //cleanMatch.show()

    val cleanUnmatch=missST.filter(!col("field").isin(execuldeList:_*)).select("field","count","mean")
    //cleanUnmatch.show()

    /* With the following code, we have a column name problem, we have duplicate column name of count, mean
    * val innerJoin=cleanMatch.join(cleanUnmatch,cleanMatch("field")===cleanUnmatch("field"),"inner")
    * innerJoin.show()
    */
    /* To solve this problem, we need to use alias for dataframfe and column*/
    val df_match=cleanMatch.as("dfmatch")
    val df_unmatch=cleanUnmatch.as("dfunmatch")

    val joinDF=df_match.join(df_unmatch,col("dfmatch.field")===col("dfunmatch.field"),"inner")
    //joinDF.select("dfmatch.count","dfunmatch.count","dfmatch.mean","dfunmatch.mean")show()
    val featureDf=joinDF.withColumn("total",col("dfmatch.count")+col("dfunmatch.count"))
                          .withColumn("delta",col("dfmatch.mean")-col("dfunmatch.mean"))
                          .select("dfmatch.field","total","delta")
    featureDf.show()

    // Create temp view for sql query
    /* matchST.createOrReplaceTempView("match_desc")
     missST.createOrReplaceTempView("miss_desc")*/
    /*spark.sql("""select * from match_desc limit 5""").show()*/
    /*spark.sql(
      """SELECT a.field, a.count + b.count total, a.mean - b.mean delta FROM match_desc a INNER JOIN miss_desc b ON a.field = b.field WHERE a.field NOT IN ("id_1", "id_2") ORDER BY delta DESC, total DESC""").show()
*/

/* Conclusion: some times sql is much easier and shorter to write*/
  }

  def ProductionExample(df:DataFrame):Unit={
    import df.sparkSession.implicits._
    //we can use as function to transform dataframe to a dataset
    val matchData = df.as[MatchData]
    matchData.show()

    /* For our scoring function, we are going to sum up the value of one field of type
* Option[Double] (cmp_lname_c1) and four fields of type Option[Int] (cmp_plz,
* cmp_by, cmp_bd, and cmp_bm). Let’s write a small helper case class to cut down on
* some of the boilerplate code associated with checking for the presence of the Option values
*
**/
  }

  /* The Score case class starts with a value of type Double (the running sum) and defines a \+ method that
  * allows us to merge an Option[Int] value into the running sum by getting the value of the Option or
  * returning 0 if it is missing. Here, we’re taking advantage of the fact that Scala lets you define
  * functions using a much broader set of names than Java to make our scoring function a bit eaiser to read:*/
  case class Score(value:Double){
    def +(oi:Option[Int])={
      Score(value+oi.getOrElse(0))
    }
  }

  case class MatchData(
                      id_1:Int,
                      id_2:Int,
                      cmp_fname_c1:Option[Double],
                      cmp_fname_c2:Option[Double],
                      cmp_lname_c1:Option[Double],
                      cmp_lname_c2:Option[Double],
                      cmp_sex:Option[Int],
                      cmp_bd:Option[Int],
                      cmp_bm:Option[Int],
                      cmp_by:Option[Int],
                      cmp_plz:Option[Int],
                      is_match:Boolean
                      )

}
