package org.pengfei.Lesson17_Analyze_Clinical_Data

import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

import scala.collection.immutable.ListMap

object Realism_Data_Curation {

  /************************************************************************************************************
    * ***************************** 17.1 Introduction *********************************************************
    * *********************************************************************************************************/
  /* In this Lesson, we will learn how to transform a dataset into a specific format. The raw dataset is provided by
   * a hospital study, we need to transform it and put it in a bio data warehouse named transmart. Transmart is not a
   * real data warehouse in a computer scientist opinion but its close enough. In this lesson, we will learn:
   * - read data from excel
   * - build new columns based on duplicate rows,
   * - change cell values of a columns
   * - deal with duplicates rows
   * - deal with null values*/

  /******************************************* Configuration ***************************************/

  val csvFile="/DATA/data_set/spark/basics/Lesson17_Analyse_Clinical_Data/raw_data.csv"
  val outputPath="/DATA/data_set/spark/basics/Lesson17_Analyse_Clinical_Data"
  val nullValue="null"
  val timePointColName="Time_Point"
  val patientIdColName="Patient"
  val separator="_"

  //config for output csv to match with transmart requirements
  val studyID="Realism01"
  val subjID="SUBJ_ID"
  val outputCsvDelimiter="\t"

  def main(args:Array[String]):Unit= {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val spark = SparkSession.builder().appName("Realism_Data_Curation").master("local[2]").getOrCreate()
    import spark.implicits._





    /******************************************************************************************************
      * ************************************17.2 Preliminary analyze on raw data *****************************
      * ******************************************************************************************************/

    /* Before we start the transformation, we need to understand the data. */

    val csvDF=spark.read.option("inferSchema", true).option("header",true)
      .option("nullValue"," ")
      .option("encoding", "UTF-8")
      .option("delimiter",",").csv(csvFile)

    csvDF.cache()
    //csvDF.show(1)

    /**********************get raw data size (i.e. rows and columns)************************/
    /*val columnNum=csvDF.columns.length
    val rowNum=csvDF.count()

    println(s"csv column Num is $columnNum, row number is $rowNum")*/

    /* csv column Num is 470, row number is 117124 */
    //csvDF.printSchema()

    /*********************** get rows of each patient and all possible row numbers*********************/
    /* With the below code, we know we have multiple rows for one patient, but the target warehouse only allow one
    * row for each distinct patient, now we need to know why we have multiple rows.*/
    val patientRowCount=csvDF.groupBy("Patient").count()

    //patientRowCount.select("count").distinct().show(10)
    /* all possible patient row number
    |  188|
    |   94|
    |  235|
    |  141|
    |  329|
    |  282|
    |   47|*/

    /****************** Discover the data is time related  *********************************/

    /* We find the column TP_Class describes the date of when the data is recorded, so we can conclude that the data
    * is time related*/
    val timePointValues=csvDF.select("TP_Class").distinct().orderBy($"TP_Class".asc)


    // timePointValues.show()
    /*
        *  D28|
    |     D5-D7
    |     D60|
    |      HV|
    |   D1-D2|
    |      D1|
    |   D3-D4|
    |      D2|
    |      D0|
    |     D14|
        * */

    /****************** Discover that all the patients belong to 5 sub group  *********************************/

    /******************************* get all possible group *******************************/
    val allGroups=csvDF.select("Subgroup").distinct()
    // allGroups.show(10)

    /********************************** get all group patient row count ************************/

    val allGroupsCount=csvDF.groupBy("Subgroup").count()
    // allGroupsCount.show(10)

    /*
+--------+-----+
|Subgroup|count|
+--------+-----+
|      HV| 8225|
|  Sepsis|26696|
|  Trauma|42253|
|    Burn| 5969|
| Surgery|33981|
+--------+-----+
    * */

    /********************* Analysis patient of each sub group *********************************/

    /* We do a raw analysis on Healthy patient HV, we discover that Healthy patient only done 1 medical visit, so each
    * patient has 47 row, which correspond the 47 marker, we will not show the result of other groups here.
    * */

    // val HV=getStatsOfEachSubGroup(csvDF,"HV")
    /*val hvFile="/DATA/data_set/spark/basics/Lesson17_Analyse_Clinical_Data/realsim/HV/HV.csv"
  val HV=spark.read.option("inferSchema", true).option("header",true).option("nullValue"," ").option("delimiter",",").csv(hvFile)*/
    /*val HVMarkerCount=HV.filter($"Patient"===5001).select("Marker").distinct().count()
    println(s"The marker count is ${HVMarkerCount}")*/
    /* The marker count is 47, so we know that for HV, each row represents a different marker */
    //val tmp=HV.groupBy("Patient").reduceGroups

    //HVMarker.show(5)

    /*Platform */

    /* val platformCount=HV.filter($"Patient"===5001).select("Platform").distinct()
     platformCount.show(10)

     val platformMarkerRollup=HV.filter($"Patient"===5001).rollup($"Platform",$"Marker").agg(first("Value").as("Value")).sort(asc("Platform"))
     platformMarkerRollup.show(47)*/

    /* val gene=HV.select("Patient","Platform","Marker","Value")
     gene.show(10)*/

    /*HV.coalesce(1).write.mode(SaveMode.Overwrite)
      .option("header","true")
      .option("mapreduce.fileoutputcommitter.marksuccessfuljobs","false") //Avoid creating of job success files
      .option("encoding", "UTF-8")
      .csv(outputPath+"/HV")*/

    /* All HV patient has 47 rows
+-----+
|count|
+-----+
|   47|
+-----+*/

    /* All HV have 1 time point HV (i.e. day 1)
    * +--------+
|TP_Class|
+--------+
|      HV|
+--------+
    * */










    /***********************************************************************************************************
      * ************************************17.3 Change date value for easier sorting *****************************
      * ******************************************************************************************************/

    /* We noticed that with the current time point column, when we do sorting, the value does not sort in good order
    *  So we need to change the value as shown below
    * */
    /*
| D0->D00|
| D1->D01|
| D1-D2->D01-D02|
| D14->D14|
| D2->D02|
| D28->D28|
| D3-D4->D03-D04|
| D5-D7->D05-D07|
| D60->D60|
| HV->D00|
  * */

    val dfWithTP= ModifyTimePoint(csvDF)

    /***********************************************************************************************************
      * ************************** 17.4 Build column Based on the patient time point row ***************************
      * ******************************************************************************************************/

    /* As our raw data has multiple rows on a patient, each row represent specific data collect at a specific time point.
    * We have two different scenarios :
    * Scenario 1. We don't have the explicite column name, we need to build column name for each value
    * for example, we have  Patient_id | Time_Point | Coag_Sofa_Score
    *                              1004
    *                                |-- D0
    *                                     |- v1
    *
    *                                |-- D1
    *                                     |- v2
    * All the rows in which columns such as age, sex, etc will have duplicate data for patient 1004
    * To eliminate all duplicate data and make data more easier to load into the data warehouse, we need to
    * transform all the rows into columns
    * For example, the new dataframe should looks like
    *                    Patient_id | D0_Coag_Sofa | D1_Coag_Sofa_Score
    *                      1004     |     v1       |     v2
    *
    * Scenario 2. We have column name in the row, for example
    * Patient_id | Time_Point | marker_name | marker_value
    *     1004
    *            |-- D0
    *                         |- n1         | v1
    *                         |- n2         | v2
    *                         |- ...
    *            |-- D1
    *                         |- n1         | v1
    *
    * The output must be
    *
    * * Patient_id | D0_n1 | D0_n2 | ... | D1_d1n1 | ...
    *     1004     |   v1  |  V2   | ... |  d1v1
    *
    *
    * */



    /***********************************************************************************************************
      * ************************** 17.4.1 SOFA time point related data treatment ***************************
      * ******************************************************************************************************/

    /* SOFA data is in scenario 1 */
    /*val allColumns=Array(patientIdColName,timePointColName,"CBD_Cardio_SOFA","CBD_Coag_SOFA",
      "CBD_Dobut_SOFA","CBD_Hepat_SOFA",
      "CBD_Neuro_SOFA","CBD_Renal_SOFA",
      "CBD_Resp_SOFA","CBD_SOFA_NA","CBD_SOFA")

      val sofaValueColumns=Array("CBD_Cardio_SOFA","CBD_Coag_SOFA",
      "CBD_Dobut_SOFA","CBD_Hepat_SOFA",
      "CBD_Neuro_SOFA","CBD_Renal_SOFA",
      "CBD_Resp_SOFA","CBD_SOFA_NA","CBD_SOFA")
      */
    /*val sofaValueColumns=Array("CBD_Cardio_SOFA","CBD_Coag_SOFA")*/
    val sofaValueColumns=Array("CBD_Cardio_SOFA","CBD_Coag_SOFA",
      "CBD_Dobut_SOFA","CBD_Hepat_SOFA",
      "CBD_Neuro_SOFA","CBD_Renal_SOFA",
      "CBD_Resp_SOFA","CBD_SOFA_NA","CBD_SOFA")
    val utilityColumns=Array(patientIdColName,timePointColName)


    /*// build a small test dataset
    val patient1088=dfWithTP.filter($"Patient"===1088)
    val sofaTest=BuildColumnsWithTimePointS1(patient1088,sofaValueColumns,utilityColumns)
    val sofaTestRowNum=sofaTest.count()
    val sofaTestColNum=sofaTest.columns.length
    println(s"sofa row number is ${sofaTestRowNum}, sofa column number is ${sofaTestColNum}")*/

    /*val sofaTPData=BuildColumnsWithTimePointS1(dfWithTP,sofaValueColumns,utilityColumns)
    val sofaRowNum=sofaTPData.count()
    val sofaColNum=sofaTPData.columns.length*/

    // sofa row number is 552, sofa column number is 82
    // println(s"sofa row number is ${sofaRowNum}, sofa column number is ${sofaColNum}")



    /***********************************************************************************************************
      * ************************** 17.4.2 CBD time point related data treatment ***************************
      * ******************************************************************************************************/

    val cbdValueColumns = Array("CBD_AdreN","CBD_AdreN_Dose","CBD_NorAdreN","CBD_NorAdreN_Dose",
      "CBD_Dobut_Dose","CBD_ALAT","CBD_ASAT","CBD_Bilirub","CBD_Creat","CBD_Diuresis","CBD_Eosino","CBD_Leuco",
      "CBD_Lympho","CBD_Mono","CBD_Neutro","CBD_FiO2", "CBD_PaO2FiO2","CBD_MAP","CBD_PAL",
      "CBD_Plat","CBD_Hb","CBD_Lactate","CBD_pH","CBD_Glasgow","CBD_Presence_Mecha_Ventil","CBD_PCT")

    /*val cbdTPData=BuildColumnsWithTimePointS1(dfWithTP,cbdValueColumns,utilityColumns)

    cbdTPData.show(5,false)
    val cbdRowNum=cbdTPData.count()
    val cbdColNum=cbdTPData.columns.length

    //cbd row number is 552, cbd column number is 235
    println(s"cbd row number is ${cbdRowNum}, cbd column number is ${cbdColNum}")*/



    /***********************************************************************************************************
      * ************************** 17.4.3 BioMarker time point related data treatment ***************************
      * ******************************************************************************************************/
    /* BioMarker data is in scenario 2*/
    /* raw dataframe
    +-------+----------+---------------+--------+----------------+------------------+-------------+
    |Patient|Time_Point|       Platform|  Marker|           Value|Missing_Value_Type|Imputed_Value|
    +-------+----------+---------------+--------+----------------+------------------+-------------+
    |   5001|       D00|      ELLA_EDTA|     IL6|           1,964|              null|            0|
    |   5001|       D00|ELLA_TRUCULTURE| IL2_NUL|            null|              OOR<|            0|

    result dataframe (build 3 new column)
    Column name rules: platform-BioMarker-TimePoint-(value|Missing_Value_Type|Imputed_Value)
    +-------+--------------------------+
    |Patient|   ELLA_EDTA-IL6-D00-value|  ELLA_EDTA-IL6-D00-Missing_Value_Type | ELLA_EDTA-IL6-D01-Imputed_Value
    +-------+--------------------------+
    |   5001|                    1,964|           null                     |  0(0->false, 1->true)

    -Biomarker
    |--Platform
      |--ELLA_EDTA
           |--IL6
               |--D0
                   | value
                   | Missing_Value_Type
                   | Imputed_Value
               |--D1
               |--...
           |--IL10
      |--ELLA_TRUCULTURE
           |--TNFa_LPS
           |--...


    So the total column_number= distinct(platform+marker)*timePoint*3

    */

    /*// build a small test dataset
    val patient1088=dfWithTP.filter($"Patient"===1088)
    //tested with different platform sub dataset, for example, ELLA_EDTA only has two rows for patient 1088
    val markerTestDf=patient1088.filter($"Platform"==="ELLA_TRUCULTURE")

    markerTestDf.show(5,false)
    val test=BuildColumnsWithTimePointS2(markerTestDf)

    test.show(5, false)*/

    /* Test with full dataset*/
    /* val bioMarkerWithTPColData=BuildColumnsWithTimePointS2(dfWithTP)
    bioMarkerWithTPColData.show(5,false)
    val rowNum=bioMarkerWithTPColData.count()
    val colNum=bioMarkerWithTPColData.columns.length
    // The row number is 552, the column number is 1270, the row number is 552 which is correct of the total patient number
    println(s"The row number is ${rowNum}, the column number is ${colNum}")*/


    /***********************************************************************************************************
      * ******************************************* 17.5 Export data ******************************************
      * ******************************************************************************************************/
    ExportDemographicData(csvDF)
    //ExportSeverityRiskFactor(csvDF)

    //ExportSofaD1Data(csvDF)
    //ExportSofaTPData(dfWithTP)
    //ExportDataWithoutTP(csvDF)

  }
  /**
    * This method transform the raw data of bioMarker Value/Missing_Value_Type/Imputed_Value to column with platform
    * name, marker name and time point
    *
    * @author Pengfei liu
    * @version 1.0
    * @since 2018-12-28
    * @param rawDf The source data frame in which the transformation will take place.
    * @return a data frame which contains the result of the transformation
    * */
  def BuildColumnsWithTimePointS2(rawDf:DataFrame):DataFrame={
    val spark=rawDf.sparkSession
    import spark.implicits._
    /* In our case, the column which we want to transform are fixed, and we only called it once, so no need to set in
    * the config. */
    val bioMarkerValueCol=Array("Value","Missing_Value_Type","Imputed_Value")
    val bioMarkerFiledIdCol=Array("Platform","Marker")
    val bioMarkerUtilityCol=Array(patientIdColName,timePointColName)
    val bioMarkerCol=bioMarkerUtilityCol.union(bioMarkerFiledIdCol).union(bioMarkerValueCol)



    /*bioMarkerData.printSchema()
    val allValue=bioMarkerData.count()
    val nonNullValue=bioMarkerData.filter($"Value".isNotNull).count()
    println(s"All value count is ${allValue}, nonNullValue count is ${nonNullValue}")*/



    /* val allPlateformMarkerTP=bioMarkerDataWith3FiledIdName.select("marker_tp").distinct().collect().toArray

     // All possible platform biomarker and time point combination number is 423, so we will have 423*3 more columns
     println(s"All possible platform biomarker and time point combination ${allPlateformMarkerTP.length}")*/

    /*Step 0: clean the raw dataset, get only biomarker related columns and fill the null value with string "null"*/
    val bioMarkerData=rawDf.select(bioMarkerCol.head,bioMarkerCol.tail:_*).dropDuplicates().orderBy(asc(patientIdColName))

    val df=bioMarkerData.na.fill(nullValue)
    df.show(5)

    val bioMarkerDataWith3FiledIdName=df.withColumn("tmp",concat(col(bioMarkerFiledIdCol(0)),lit(separator),col(bioMarkerFiledIdCol(1))))
      .withColumn("marker_tp",concat($"tmp",lit(separator),col(timePointColName)))
      .drop("tmp")
      .withColumn("marker_Value",concat($"marker_tp",lit("/Value")))
      .withColumn("marker_Missing_Value_Type",concat($"marker_tp",lit("/Missing_Value_Type")))
      .withColumn("marker_Imputed_Value",concat($"marker_tp",lit("/Imputed_Value")))

    bioMarkerDataWith3FiledIdName.show(5,false)

    var result=df.select(patientIdColName).distinct().sort(asc(patientIdColName))

    for(filedValueColName<-bioMarkerValueCol){
      println(s"Current working column name : ${filedValueColName}")
      val filedIdColName="marker_"+filedValueColName
      val inter=RowToColumn(bioMarkerDataWith3FiledIdName,patientIdColName,filedIdColName,filedValueColName)
      result=result.join(inter,Seq(patientIdColName),"inner")
    }
    result.show(1, false)

    //Sort the output column so the Value/Missing_Value_Type/Imputed_Value of a marker are together

    val sortedColumnName=Array("Patient")++result.columns.sorted.filter(!_.equals("Patient"))

    println(s"The sorted Column Name is ${sortedColumnName.mkString(";")}")

    result=result.select(sortedColumnName.head,sortedColumnName.tail:_*)

    return result


  }


  /**
    * This method transform the raw data of scenario 1 to column with time point
    *
    * @author Pengfei liu
    * @version 1.0
    * @since 2018-12-20
    * @param df The source data frame in which the transformation will take place.
    * @param allColumns  allColumns is a list of String which contains all the column name
    *                           the name of the newly created column name.
    * @param utilityColumns utilityColumns
    *                              become a new row in the corresponding filed column
    * @return a data frame which contains the result of the transformation
    * */
  def BuildColumnsWithTimePointS1(df:DataFrame,valueColumns:Array[String],utilityColumns:Array[String]):DataFrame={
    /* change row to column with time_point in consideration
    * here is the origin data frame
    *
    +-------+----------+---------------
    |Patient|Time_Point|CBD_Cardio_SOFA|
    +-------+----------+---------------+
    |   1004|   D03-D04|              1|
    |   1004|   D05-D07|              1|
    |   1004|       D28|           null|
    |   1004|   D01-D02|              4|
    |   1007|    ...
    *
    * Step.1 .  Build filedId column
    *
    |Patient|CBD_Cardio_SOFA_ID|CBD_Cardio_SOFA_Value|
    +-------+----------+---------------+
    |   1004|   CBD_Cardio_SOFA.D03-D04|           1 |
    |   1004|   CBD_Cardio_SOFA.D05-D07|           1 |
    |   1004|       CBD_Cardio_SOFA.D28|         null|
    |   1004|   CBD_Cardio_SOFA.D01-D02|           4 |
    |   1007|    ...
    * */
    val spark=df.sparkSession
    import spark.implicits._
    /*Step1. if filedId column does not exit, create filedId column */

    //Get all filed value column name
    val allColumns=valueColumns.union(utilityColumns)

    println(s"allColumns ${allColumns.mkString(";")}")

    //Get all time point

    val allColumnData=df.select(allColumns.head,allColumns.tail:_*).dropDuplicates().orderBy(asc(patientIdColName))

    allColumnData.show(10)

    /* no need to get the array of timePoint
    val timePoint=sofa_TPData.select("TP_Class").distinct().collect().map(_.get(0))*/
    var tmp=allColumnData
    for(valueColumn<-valueColumns){
      tmp=tmp.withColumn("tmp",lit(valueColumn))
        /* do not put . in the column name, spark will think you want to access an attribute of the columne*/
        .withColumn(valueColumn+"_Id",concat($"tmp",lit(separator),col(timePointColName)))
        .drop("tmp")
    }
    tmp.show(10)
    // tmp.printSchema()

    /* Here we need to loop over all elements in value column,*/
    var result=tmp.select(patientIdColName).distinct().sort(asc(patientIdColName))
    result.show(5)
    for(filedValueColumn<-valueColumns){
      val filedColumnId=filedValueColumn+"_Id"

      val inter=RowToColumn(tmp,patientIdColName,filedColumnId,filedValueColumn)
      result=result.join(inter,Seq(patientIdColName),"inner")
      result.show(10)
    }


    return result
  }

  /**
    * This method transform multi rows of an object into columns, after the transformation, for each object we only have
    * one row in the data frame. To make the transformation, this method cast all column to type string, as we don't do
    * arthimetic operations here. So it won't be a problem. You can change the type back to Int, or double after the
    * transformation.
    * @author Pengfei liu
    * @version 1.0
    * @since 2018-12-20
    * @param df The source data frame in which the transformation will take place.
    * @param targetIdColumnName The column in the data frame which contains the name of the filed. Each row will become
    *                           the name of the newly created column name.
    * @param targetValueColumnName The column in the data frame which contains the value of the filed. Each row will
    *                              become a new row in the corresponding filed column
    * @return a data frame which contains the result of the transformation
    *
    * */

  def RowToColumn(rawDf: DataFrame, objectIdColumnName:String,targetIdColumnName: String,targetValueColumnName:String): DataFrame = {
    val spark = rawDf.sparkSession
    import spark.implicits._

    /* Step0. Eliminate all null rows, it may create a null dataframe (dataframe without rows), we can't build columns
    * with no rows, so we need to fill null with a null value which will not cause null pointer exception.
    * As a result, we cast all columns to string type and fill the null cell with pre-defined nullValue(String)*/
    val df = rawDf.select(rawDf.columns.map(c => col(c).cast(StringType)) : _*).na.fill(nullValue)

    /* Step1. Get all possible filedIDs, which will be become the column name of each filed */

    val filedIDs = df.select(targetIdColumnName).distinct().orderBy(df(targetIdColumnName).asc)

    filedIDs.show(10,false)

    // convert the column in the data frame which contains the filed Ids to an Array of the filed Ids.
    val filedIDsArray: Array[String] = filedIDs.collect().map(_.get(0).toString)



    /* Step2. Build the (filedId,filedValue) <key,value> map for each row.  */

    /* We have two solutions to do this.
    * Solution 1 : build a user define function which build a map
    * Solution 2 : Spark provide map function which can build a map based on two columns
    * Here we choose Solution 2 , spark native function is always better than udf.*/

    // Solution 1: If we don't fill null value before, here we need to use Option type to avoid null pointer
    /*def buildFiledMap(filedName:String,filedValue:String):Map[String,Option[String]]={
      if(filedValue.isEmpty) Map(filedName->None)
      else Map(filedName->Option(filedValue))
    }
    spark.udf.register("buildFiledMap",(arg1:String,arg2:String)=>buildFiledMap(arg1,arg2))
    val filedIdValueMap=df.withColumn("filed_map",expr(s"buildFiledMap(${targetIdColumnName},${targetValueColumnName})"))*/

    /* def buildFiledMap(filedName:String,filedValue:String):Map[String,String]={
      if(filedValue.isEmpty) Map(filedName->"null")
      else Map(filedName->filedValue)
    }
    spark.udf.register("buildFiledMap",(arg1:String,arg2:String)=>buildFiledMap(arg1,arg2))
    val filedIdValueMap=df.withColumn("filed_map",expr(s"buildFiledMap(${targetIdColumnName},${targetValueColumnName})"))
    */

    /* Solution 2 : The spark native map function
    *  The map function by default does not deal with null value, so if we have null value in the two columns you will
    *  have x->, or ->y, when you have functions to call these null values, you will have null pointer exception.
    *  The solution is to fill the null value with a string "null",
    **/
    val filedIdValueMap = df.withColumn("filed_map", map(df(targetIdColumnName), df(targetValueColumnName)))

    filedIdValueMap.show(5,false)

    /* Step3. Group the (filedId,filedValue) map for each distinct subject which may have multiple rows. Each row has
    * a map. After group, we concatenate all maps of a subject into one single map. Here, we used collect_list, there is
    * another similar function collect_set, which list returns an ordered sequence of elements, set returns an unordered
    * distinct list of elements, we know that, we will not have duplicate filedId for one subject. so we don't need to use
    * set, we prefer to use list.*/
    val groupedFiledIdValueMap = filedIdValueMap.groupBy(objectIdColumnName)
      .agg(collect_list("filed_map")) // return a list of map
      .as[(String, Seq[Map[String, String]])] // <-- leave Rows for typed pairs
      .map { case (id, list) => (id, list.reduce(_ ++ _)) } // <-- concatenate all maps to a single map
      .toDF(objectIdColumnName, "filed_map")

    groupedFiledIdValueMap.show(10, false)


    /* Step 4. Create column for each fieldId based on the complete fieldId list, with the getFiledValue function,
    * */
    val bFiledIDsArray: Broadcast[Array[String]] = spark.sparkContext.broadcast(filedIDsArray)

    def getFiledValue(filedId: String, filedMap: Map[String, String]): String = {
      //you can replace the empty (null) value as you want, here I tried empty string "", "null" and "."
      if(filedMap.isEmpty||filedId.isEmpty){nullValue}
      else {
        filedMap.getOrElse(filedId, nullValue)
      }
    }

    //spark.udf.register("getFiledValue", (arg1: String, arg2: Map[String, String]) => getFiledValue(arg1, arg2))
    spark.udf.register("getFiledValue", getFiledValue(_:String, _: Map[String, String]))

    var tmpDf = groupedFiledIdValueMap

    (0 until bFiledIDsArray.value.length).map { i =>
      val filedId: String = bFiledIDsArray.value(i)
      tmpDf = tmpDf.withColumn("current_id", lit(filedId))
        .withColumn(filedId, expr("getFiledValue(current_id,filed_map)"))
        .drop("current_id")

      // The solution which takes a variable and a column does not work, because, the udf only allows column type as argument
      //
      //tmpDf=tmpDf.withColumn(filedId,getFiledValue(filedId,filed_map)))
    }

    val result=tmpDf.drop("filed_map")
    result.show(5,false)
    result
  }


  /*************************************************************************************************************
    * *************************** 17.5.1 Prepare not time related data and export them ***************************
    * ******************************************************************************************************/

  /******************** 17.5.1.1 Prepare demographic data and export them ******************/

  def ExportDemographicData(df:DataFrame):Unit={
    val spark=df.sparkSession;
    import spark.implicits._

    // prepare demographicColumns, as demographicColumns are not time point related, so with drop duplicates, we get one
    // row per patient

    val demographicColumns=Array("Patient","Subgroup","DD_Gender","DD_Calculated_Age","DD_Height","DD_Weight","DD_BMI")

    val demographicData=df.select(demographicColumns.head, demographicColumns.tail: _*).dropDuplicates().orderBy($"Patient".asc)

    demographicData.show(10)
    // column rename map
    val nameMap=Map(("DD_Gender","Sex"),("DD_Calculated_Age","Age"),("DD_Height","Height"),("DD_Weight","Weight"),("DD_BMI","BMI"))

    /* Step 1 : normalize data for transmart format*/

    val demoForTransmart=NormalizeColNameForTransmart(demographicData,demographicColumns)
    demoForTransmart.show(10,false)

    /* Step 2 : change column name*/

    val demoRenamedDf=ChangeColName(demoForTransmart,nameMap)
    demoRenamedDf.show(10,false)

    /* Step 3 : check null value */
    countNullValue(demoRenamedDf)

    /* The null value count of the column STUDY_ID is 0
  The null value count of the column SUBJ_ID is 0
  The null value count of the column Subgroup is 0
  The null value count of the column Sex is 0
  The null value count of the column Age is 0
  The null value count of the column Height is 6
  The null value count of the column Weight is 3
  The null value count of the column BMI is 6*/

    /* Step 4 : fill null value with transmart required value (. for digit, Not Available for string)*/
    /* We know Height, Weight, BMI are all digit columns, so we replace them with .
    * ,"Weight","BMI"*/
    val demoFinalData=fillTransmartNullForDigitCol(demoRenamedDf,Array("Height"),nullValue)
    countNullValue(demoFinalData)

    /* Step 5 : output data to disk */
    WriteDataToDisk(demoFinalData,"/tmp/Realism","demographic_data")

    // Get the first line for each patient, We have two solution, we can use .groupBy().agg(first(...),...)
    // We can also just keep one row by eliminating all duplicates rows.

    //finalDemographData.show(5)
    // You want the max/min value(first) of one column, groupBy does not guarantee the order by default,
    // you have to do sort before use first
    // example with groupBy and agg(first). But be careful, if the rows are not completely identical, the result may be
    // not reliable, because groupBy does not guarantee order, you have to do sort before first to get always the same
    // first line

    /* //Get the first element of each column
    val DemographDataFirst=demographicData.groupBy("Patient").agg(first("DD_Gender").as("Gender"),
      first("DD_Calculated_Age").as("Age"),
      first("DD_Height").as("Height"),
      first("DD_Weight").as("Weight"),
      first("DD_BMI").as("BMI")
    )
    DemographDataFirst.show(5)*/

  }
  /******************** 17.5.1.2 Prepare Severity Risk Factor data and export them ******************/

  def ExportSeverityRiskFactor(df:DataFrame):Unit={
    val spark=df.sparkSession;
    import spark.implicits._

    /********************** prepare the History_and_comorbidity/Security_and_risk_factor columns**********************/
    val severityRiskFactor=Array("Patient","SRF_ASA","SRF_ASA_NA","SRF_CEI","SRF_Coma","SRF_Diag_Cat","SRF_Inhalation","SRF_Maccabe",
      "SRF_Pulmo_Contusion","SRF_Statins","SRF_CMV_Ab","SRF_HSV1_Ab","SRF_SAPSII")

    val severityRiskFactorData=df.select(severityRiskFactor.head,severityRiskFactor.tail: _*).dropDuplicates().orderBy($"Patient".asc)
    severityRiskFactorData.show(5)

    /*Step1: normalize data for transmart format*/
    val srfData=NormalizeColNameForTransmart(severityRiskFactorData,severityRiskFactor)
    srfData.show(3,false)

    /*Step2 : change col name*/

    /*Step3 : check null col*/
    //countNullValue(srfData)

    /*Step4 : replace null with transmart required null value
    * For digit columns -> .
    * For String columns -> Not Available
    * In the column "SRF_CMV_Ab","SRF_HSV1_Ab" (digit columns), we have special value such as Neg, we also want to
    * replace it by 0.
    * */
    /* String columns : */
    val strColumns=Array("SRF_ASA_NA","SRF_CEI","SRF_Coma","SRF_Diag_Cat","SRF_Inhalation","SRF_Maccabe","SRF_Pulmo_Contusion","SRF_Statins")
    // fill string null value
    val fillStr=fillTransmartNullForStrCol(srfData,strColumns,nullValue)

    /* Digit columns : */
    val digitColumns=Array("SRF_ASA","SRF_CMV_Ab","SRF_HSV1_Ab","SRF_SAPSII")
    // fill digit null value
    val fillDigit=fillTransmartNullForDigitCol(fillStr,digitColumns,nullValue)

    // Replace Neg with 0 for col SRF_CMV_Ab and SRF_HSV1_Ab
    val finalSrfData=replaceSpecValue(fillDigit,Array("SRF_CMV_Ab","SRF_HSV1_Ab"),"Neg", "0")

    /* Step5 : output data to disk*/
    finalSrfData.show(5,false)
    WriteDataToDisk(finalSrfData,"/tmp/Realism","SRF_data")

    /*// Check SRF_CMV_Ab column . number = the number of null or not, we get 195 which is a match
    val checkColName="SRF_CMV_Ab"

    finalSrfData.select(checkColName).distinct().orderBy(finalSrfData(checkColName).desc).show(10,false)

    val SRF_CMV_count=finalSrfData.filter(finalSrfData(checkColName)===".").distinct().count()*/


    /*// The count shows no more null in all columns
    countNullValue(finalSrfData)*/
  }


  /********************** 17.5.1.4 prepare the sofa D1 Columns data and export them as csv **********************/
  def ExportSofaD1Data(df:DataFrame):Unit={
    val spark=df.sparkSession;
    import spark.implicits._

    /* sofa_D1 has all the column which are not time point related*/
    val sofaD1=Array("Patient","CBD_Cardio_SOFA_Theoretical_D1","CBD_Coag_SOFA_Theoretical_D1",
      "CBD_Dobut_SOFA_Theoretical_D1","CBD_Hepat_SOFA_Theoretical_D1","CBD_Neuro_SOFA_Theoretical_D1","CBD_Renal_SOFA_Theoretical_D1",
      "CBD_Resp_SOFA_Theoretical_D1","CBD_SOFA_Theoretical_D1")

    val sofaD1Data=df.select(sofaD1.head,sofaD1.tail:_*).dropDuplicates().orderBy($"Patient".asc)

    /*Step1: normalize data for transmart format*/
    val sofaD1NormData=NormalizeColNameForTransmart(sofaD1Data,sofaD1)
    sofaD1NormData.show(3,false)

    /*Step2 : change col name*/

    /*Step3 : check null col*/
    //countNullValue(sofaD1NormData)

    /*Step4 : replace null with transmart required null value*/
    /*String column: */
    val strColumns=Array("CBD_Dobut_SOFA_Theoretical_D1")
    val fillStr=fillTransmartNullForStrCol(sofaD1NormData,strColumns,nullValue)
    /*Digit column: */
    val digitColumns=Array("CBD_Cardio_SOFA_Theoretical_D1","CBD_Coag_SOFA_Theoretical_D1",
      "CBD_Hepat_SOFA_Theoretical_D1","CBD_Neuro_SOFA_Theoretical_D1","CBD_Renal_SOFA_Theoretical_D1",
      "CBD_Resp_SOFA_Theoretical_D1","CBD_SOFA_Theoretical_D1")
    val fillDigit=fillTransmartNullForDigitCol(fillStr,digitColumns,nullValue)

    /*check distinct value of each column, no special value found, so final data= fillDigit*/
    //getDistinctValueOfColumns(fillDigit,strColumns,10)
    // getDistinctValueOfColumns(fillDigit,digitColumns,10)

    val finalSofaD1Data=fillDigit
    /*Step5 : output data to disk*/
    WriteDataToDisk(finalSofaD1Data,"/tmp/Realism","SofaD1_data")
  }

  /******************* 17.5.1.5 prepare the sofa with time point Columns data and export them as csv *****************/
  def ExportSofaTPData(df:DataFrame):Unit={
    val spark=df.sparkSession;
    import spark.implicits._

    val sofaValueColumns=Array("CBD_Cardio_SOFA","CBD_Coag_SOFA",
      "CBD_Dobut_SOFA","CBD_Hepat_SOFA",
      "CBD_Neuro_SOFA","CBD_Renal_SOFA",
      "CBD_Resp_SOFA","CBD_SOFA_NA","CBD_SOFA")
    val utilityColumns=Array(patientIdColName,timePointColName)
    val allColumns=utilityColumns++sofaValueColumns

    val sofaRawData=df.select(allColumns.head,allColumns.tail:_*).dropDuplicates().orderBy($"Patient".asc)
    /* We find out the rows of time_point D14, D28, D60, all the value columns are null, so we decide to remove
    * these rows */

    /* We can conclude the refine process is correct, we have 981 null rows in D14, D28 and D60, before refine process,
    * we have 2452 rows, after we have 1471 rows */
    val sofaRefinedData=removeRowsWithSpecValues(sofaRawData,"Time_Point",Array("D14","D28","D60"))

    sofaRefinedData.show(3,false)

    sofaRefinedData.cache()

    /*Step0: transform multi rows to columns*/
    val sofaTPData=BuildColumnsWithTimePointS1(sofaRefinedData,sofaValueColumns,utilityColumns)

    /*Step1: normalize data for transmart format*/
    val sofaTPNormData=NormalizeColNameForTransmart(sofaTPData,sofaTPData.columns.toArray)
    sofaTPNormData.show(3,false)

    /*Step2 : change col name*/

    /*Step3 : check null col, all value columns has null values, so we need to do
    * fill null on all columns */
    //countNullValue(sofaTPNormData)

    /*Step4 : replace null with transmart required null value*/
    /* string columns*/

    val strColumns=Array("CBD_Dobut_SOFA_D00","CBD_Dobut_SOFA_D01","CBD_Dobut_SOFA_D01-D02","CBD_Dobut_SOFA_D02","CBD_Dobut_SOFA_D03-D04","CBD_Dobut_SOFA_D05-D07",
      "CBD_SOFA_NA_D00","CBD_SOFA_NA_D01","CBD_SOFA_NA_D01-D02","CBD_SOFA_NA_D02","CBD_SOFA_NA_D03-D04","CBD_SOFA_NA_D05-D07")
    val fillStr=fillTransmartNullForStrCol(sofaTPNormData,strColumns,nullValue)
    /* digit columns*/
    val digitColumns=Array("CBD_Cardio_SOFA_D00","CBD_Cardio_SOFA_D01","CBD_Cardio_SOFA_D01-D02","CBD_Cardio_SOFA_D02","CBD_Cardio_SOFA_D03-D04","CBD_Cardio_SOFA_D05-D07",
      "CBD_Coag_SOFA_D00","CBD_Coag_SOFA_D01","CBD_Coag_SOFA_D01-D02","CBD_Coag_SOFA_D02","CBD_Coag_SOFA_D03-D04","CBD_Coag_SOFA_D05-D07",
      "CBD_Hepat_SOFA_D00","CBD_Hepat_SOFA_D01","CBD_Hepat_SOFA_D01-D02","CBD_Hepat_SOFA_D02","CBD_Hepat_SOFA_D03-D04","CBD_Hepat_SOFA_D05-D07",
      "CBD_Neuro_SOFA_D00","CBD_Neuro_SOFA_D01","CBD_Neuro_SOFA_D01-D02","CBD_Neuro_SOFA_D02","CBD_Neuro_SOFA_D03-D04","CBD_Neuro_SOFA_D05-D07",
      "CBD_Renal_SOFA_D00","CBD_Renal_SOFA_D01","CBD_Renal_SOFA_D01-D02","CBD_Renal_SOFA_D02","CBD_Renal_SOFA_D03-D04","CBD_Renal_SOFA_D05-D07",
      "CBD_Resp_SOFA_D00","CBD_Resp_SOFA_D01","CBD_Resp_SOFA_D01-D02","CBD_Resp_SOFA_D02","CBD_Resp_SOFA_D03-D04","CBD_Resp_SOFA_D05-D07",
      "CBD_SOFA_D00","CBD_SOFA_D01","CBD_SOFA_D01-D02","CBD_SOFA_D02","CBD_SOFA_D03-D04","CBD_SOFA_D05-D07")

    val fillDigit=fillTransmartNullForDigitCol(fillStr,digitColumns,nullValue)

    /*check distinct value of each column, no special value found, so final data= fillDigit*/
    //getDistinctValueOfColumns(fillDigit,strColumns,10)
    // getDistinctValueOfColumns(fillDigit,digitColumns,10)

    val finalSofaTPData=fillDigit
    /*Step5 : output data to disk*/
    WriteDataToDisk(finalSofaTPData,"/tmp/Realism","SofaTP_data")

  }

  def ExportDataWithoutTP(df:DataFrame):Unit={

    val spark=df.sparkSession;
    import spark.implicits._


    val demographicColumns=Array("Patient","Subgroup","DD_Gender","DD_Calculated_Age","DD_Height","DD_Weight","DD_BMI")

    val severityRiskFactor=Array("Patient","SRF_ASA","SRF_ASA_NA","SRF_CEI","SRF_Coma","SRF_Diag_Cat","SRF_Inhalation","SRF_Maccabe",
      "SRF_Pulmo_Contusion","SRF_Statins","SRF_CMV_Ab","SRF_HSV1_Ab","SRF_SAPSII")




    /************************ prepare the History_and_comorbidity/Charlson columns ***************************/
    val charlson=Array("Patient","Charlson_AIDS","Charlson_Cerebrovasc_Disease","Charlson_Chronic_Pulm_Disease",
      "Charlson_Congest_Heart_Failure","Charlson_Connect_Tissue_Disease","Charlson_Dementia","Charlson_Hemiplegia",
      "Charlson_Kidney_Disease","Charlson_Leukemia","Charlson_Liver_Disease","Charlson_Malign_Lymphoma",
      "Charlson_Mellitus_Diabetes", "Charlson_Myocardial_Inf","Charlson_Peptic_Ulcer_Disease",
      "Charlson_Periph_Vascular_Disease","Charlson_Solid_Tumor","Charlson_Score")

    val charlsonData=df.select(charlson.head,charlson.tail:_*)
    // charlsonData.show(5)
    // charlsonData.dropDuplicates().show()

    /************************** Prepare the History_and_comorbidity/SOFA, SOFA is time point related.***************/
    val sofa=Array("Patient","TP_Class","CBD_Cardio_SOFA","CBD_Cardio_SOFA_Theoretical_D1","CBD_Coag_SOFA","CBD_Coag_SOFA_Theoretical_D1",
      "CBD_Dobut_SOFA","CBD_Dobut_SOFA_Theoretical_D1","CBD_Hepat_SOFA","CBD_Hepat_SOFA_Theoretical_D1",
      "CBD_Neuro_SOFA","CBD_Neuro_SOFA_Theoretical_D1","CBD_Renal_SOFA","CBD_Renal_SOFA_Theoretical_D1",
      "CBD_Resp_SOFA","CBD_Resp_SOFA_Theoretical_D1","CBD_SOFA_NA","CBD_SOFA","CBD_SOFA_Theoretical_D1")

    val sofaData=df.select(sofa.head,sofa.tail:_*)
    // sofaData.show(5)
    // sofaData.dropDuplicates().show()

    /*************************** prepare the History_and_comorbidity/Clinical_biological_data ************************/

    /* CBD is time related too */

    val clinicalBiological=Array("Patient","TP_Class","CBD_AdreN","CBD_AdreN_Theoretical_D1","CBD_AdreN_Dose","CBD_AdreN_Dose_Theoretical_D1",
      "CBD_NorAdreN","CBD_NorAdreN_Theoretical_D1","CBD_NorAdreN_Dose","CBD_NorAdreN_Dose_Theoretical_D1",
      "CBD_Dobut_Dose","CBD_Dobut_Dose_Theoretical_D1","CBD_ALAT","CBD_ALAT_Theoretical_D1","CBD_ASAT",
      "CBD_ASAT_Theoretical_D1","CBD_Bilirub","CBD_Bilirub_Theoretical_D1","CBD_Creat","CBD_Creat_Theoretical_D1",
      "CBD_Diuresis","CBD_Diuresis_Theoretical_D1","CBD_Eosino","CBD_Eosino_Theoretical_D1","CBD_Leuco",
      "CBD_Leuco_Theoretical_D1","CBD_Lympho","CBD_Lympho_Theoretical_D1","CBD_Mono","CBD_Mono_Theoretical_D1",
      "CBD_Neutro","CBD_Neutro_Theoretical_D1","CBD_FiO2","CBD_FiO2_Theoretical_D1", "CBD_PaO2FiO2",
      "CBD_PaO2FiO2_Theoretical_D1","CBD_MAP","CBD_MAP_Theoretical_D1","CBD_PAL","CBD_PAL_Theoretical_D1",
      "CBD_Plat","CBD_Plat_Theoretical_D1","CBD_Hb","CBD_Hb_Theoretical_D1","CBD_Lactate","CBD_Lactate_Theoretical_D1",
      "CBD_pH","CBD_pH_Theoretical_D1","CBD_Glasgow","CBD_Glasgow_Theoretical_D1","CBD_Presence_Mecha_Ventil",
      "CBD_Presence_Mecha_Ventil_Theoretical_D1","CBD_PCT","CBD_PCT_Theoretical_D1")

    val clinicalBiologicalData=df.select(clinicalBiological.head,clinicalBiological.tail:_*)
    //clinicalBiologicalData.show(5)
    //clinicalBiologicalData.dropDuplicates().show(5)


    /************************* prepare  History_and_comorbidity/Administrated cares **************************/
    val administeredCares=Array("Patient","AC_Blood_Derivated_Products","AC_Mass_Blood_Transf","AC_Fresh_Frozen_Plasma","AC_Catechol",
      "AC_Catechol_Duration","AC_Catechol_D30FD","AC_HCHS","AC_HCHS_Duration","AC_HCHS_D30FD","AC_Corticotherapy_Other",
      "AC_Corticotherapy_Other_Duration","AC_Corticotherapy_Other_D30FD","AC_Continuous_RRT","AC_InterM_Hemodialysis",
      "AC_InterM_Hemodialysis_Sessions_Number","AC_RRT","AC_RRT_Duration","AC_RRT_D30FD","AC_Infection_Source_Controlled",
      "AC_Surgical_Intervention","AC_Surgical_Intervention_Nb")
    val administeredCaresData=df.select(administeredCares.head,administeredCares.tail:_*)
    //administeredCaresData.show(5)
    //administeredCaresData.dropDuplicates().show(5)


    /************************* prepare  History_and_comorbidity/Invasive devices **************************/

    val invasiveDevices=Array("Patient","ID_Intub_Tracheo","ID_Intub_Duration","ID_Intub_D30FD","ID_Reintub","ID_Mechanical_Ventilation",
      "ID_Mechanical_Ventilation_Duration","ID_Mechanical_Ventilation_D30FD","ID_Urin_Cath","ID_Urin_Cath_Duration",
      "ID_Urin_Cath_D30FD","ID_Venous_Cath","ID_Venous_Cath_Duration","ID_Venous_Cath_D30FD")

    val invasiveDevicesData=df.select(invasiveDevices.head,invasiveDevices.tail: _*)
    //invasiveDevicesData.show(5)
    //invasiveDevicesData.dropDuplicates().show(5)

    /*
    Clinical follow up  (no time related variables)
    ├─ Follow up
    ├─ Hospital Acquired Infection
      HAI_number (integer)
      HAI_Time_To_HAI1_All (integer)
      HAI_Time_To_HAI1_Definite (integer)
      HAI_Time_To_HAI1_Likely (integer)
    ├─ Group specific follow up
      ├─ Septic shock
      ├─ Severe Trauma
      ├─ Severe Burn
      └─ Major Surgery
      ├─ EQ5D
      └─ End of study

    */
    /***************************** Prepare clinical follow up/follow up *****************************************/

    val followUp=Array("Patient","FUD_ICU_Disch_Dest","FUD_ICU_Disch_Status","FUD_ICU_LOS","FUD_ICU_D30FD","FUD_Hosp_Disch_Dest",
      "FUD_Hosp_Disch_Status","FUD_Hosp_LOS","FUD_Hosp_D30FD","FUD_D30_Survival_Time","FUD_D14_Status","FUD_D28_Status",
      "FUD_D60_Status","FUD_D90_Status","FUD_Anti_Inf_D14","FUD_Anti_Inf_D28","FUD_Anti_Inf_D60","FUD_Anti_Inf_D90",
      "FUD_Chemo_D28","FUD_Chemo_D60","FUD_Chemo_D90")

    val followUpData=df.select(followUp.head,followUp.tail:_*)

    //followUpData.dropDuplicates().show(5)

    /***************************** Prepare clinical follow up/Hospital_Acquired_Infection **************************/
    val hospitalAcquiredInfection=Array("Patient","HAI_Number","HAI_Time_To_HAI1_All","HAI_Time_To_HAI1_Definite","HAI_Time_To_HAI1_Likely",
      "HAI_D30_All_Status","HAI_D30_All_Time","HAI_D30_Definite_Status","HAI_D30_Definite_Time")

    val hospitalAcquiredInfectionData=df.select(hospitalAcquiredInfection.head,hospitalAcquiredInfection.tail:_*)

    //hospitalAcquiredInfectionData.dropDuplicates().show(5)

    /***************************** Prepare clinical follow up/Group_specific_follow_up/Septic_shock *****************/

    /* Possible typo at "SS_Bacteremie_Germ2"*/
    val groupFollowUPSepsis=Array("Patient", "SS_Septic_Shock_At_Inclusion","SS_Inf_Localization","SS_Inf_Localization_Clarif","SS_Inf_Type",
      "SS_Inf_Acq_Type","SS_Germ1","SS_Germ1_Cat_Standard","SS_Germ1_Cat_Detailed","SS_Germ2","SS_Germ2_Cat_Standard",
      "SS_Germ2_Cat_Detailed","SS_Bacteremia","SS_Bacteremia_Germ1","SS_Bacteremie_Germ2")

    val groupFollowUpSepsisData=df.select(groupFollowUPSepsis.head,groupFollowUPSepsis.tail:_*)
    //groupFollowUpSepsisData.dropDuplicates().show(5)

    /***************************** Prepare clinical follow up/Group_specific_follow_up/Severe_Trauma *****************/

    val groupFollowUPTrauma=Array("Patient","ST_ISS","ST_Prophyl_Antibio_Admin")

    val groupFollowUPTraumaData=df.select(groupFollowUPTrauma.head,groupFollowUPTrauma.tail:_*)
    //groupFollowUPTraumaData.dropDuplicates().show(5)

    /***************************** Prepare clinical follow up/Group_specific_follow_up/Severe_Burn *****************/

    val groupFollowUPBurn=Array("Patient","SB_Body_Surface_Burn_Perc","SB_Revised_Baux_Score","SB_Smoke_Inhalation","SB_Burn_Prophyl_Antibio_Admin")
    val groupFollowUPBurnData=df.select(groupFollowUPBurn.head,groupFollowUPBurn.tail:_*)

    //groupFollowUPBurnData.dropDuplicates().show(5)

    /***************************** Prepare clinical follow up/Group_specific_follow_up/Major_Surgery *****************/

    val groupFollowUPSurgery=Array("Patient","MS_Intervention_Type","MS_Prophyl_ATB")
    val groupFollowUPSurgeryData=df.select(groupFollowUPSurgery.head,groupFollowUPSurgery.tail:_*)

    // groupFollowUPSurgeryData.dropDuplicates().show(5)

    /***************************** Prepare clinical follow up/Group_specific_follow_up/EQ5D *****************/

    val groupFollowUPEQ5D=Array("Patient","EQ5D_Anxiety_Depression_D9","EQ5D_Form_Completed_D90","EQ5D_Mobility_D90",
      "EQ5D_Mobility_D90","EQ5D_Pain_Discomfort_D90","EQ5D_Self_Care_D90","EQ5D_Usual_Activities_D90")
    val groupFollowUPEQ5DData=df.select(groupFollowUPEQ5D.head,groupFollowUPEQ5D.tail:_*)

    //groupFollowUPEQ5DData.dropDuplicates().show(5)

    /***************************** Prepare clinical follow up/Group_specific_follow_up/End_of_Study *****************/

    val groupFollowUPEnd=Array("Patient","ES_Prema_End","ES_Prema_Reason_End","ES_Other_Reason_Prema_End",
      "ES_Exclusion","ES_Specimen_Destruction")
    val groupFollowUPEndData=df.select(groupFollowUPEnd.head,groupFollowUPEnd.tail:_*)

    // groupFollowUPEndData.dropDuplicates().show(5)
    /******************************* Join dataframe test *************************/
    // The following code will result a dataframe with two column of Patient

    /* val joinTest=groupFollowUPEQ5DData.join(groupFollowUPEndData,groupFollowUPEQ5DData("Patient")===groupFollowUPEndData("Patient"),"inner")
     joinTest.show(5)
     */

    /* With the following code, we only have one Patient column*/
    //val joinTest=groupFollowUPEQ5DData.join(groupFollowUPEndData,Seq("Patient"),"inner")
    //joinTest.show(5)

    /******************************* join the column and do a new select ******/
    // use column union to get all data without time point
    val allColumnsWtihoutTP=demographicColumns
      .union(severityRiskFactor)
      .union(charlson)
      .union(administeredCares)
      .union(invasiveDevices)
      .union(followUp)
      .union(hospitalAcquiredInfection)
      .union(groupFollowUPSepsis)
      .union(groupFollowUPTrauma)
      .union(groupFollowUPBurn)
      .union(groupFollowUPSurgery)
      .union(groupFollowUPEQ5D)
      .union(groupFollowUPEnd).distinct
    val columnUnionTest=df.select(allColumnsWtihoutTP.head,allColumnsWtihoutTP.tail:_*).dropDuplicates()

    //columnUnionTest.count()
    // 552 row (Patient) in total
    val columnNumWithoutTP=columnUnionTest.columns.length
    //columnUnionTest.orderBy($"Patient".asc).show(2)
  }

  /*********************************** 17.3 Change date value for easier sorting *******************************/

  def ModifyTimePoint(df:DataFrame):DataFrame={
    val spark=df.sparkSession
    spark.udf.register("changeTimePoint",(timePoint:String)=>changeTimePoint(timePoint))
    val dfWithNewTimePoint=df.withColumn("Time_Point",expr("changeTimePoint(TP_Class)"))
    dfWithNewTimePoint.select("TP_Class","Time_Point").distinct().show(10)

    /*dfWithNewTimePoint.coalesce(1).write.mode(SaveMode.Overwrite)
    .option("header","true")
    .option("mapreduce.fileoutputcommitter.marksuccessfuljobs","false") //Avoid creating of crc files
    .option("encoding", "UTF-8")
    .csv(outputPath+"/TimePoint")*/
    return dfWithNewTimePoint
  }

  def changeTimePoint(timePoint:String):String={
    timePoint match {
      case "D0" => "D00"
      case "D1" => "D01"
      case "D1-D2" => "D01-D02"
      case "D2" => "D02"
      case "D3-D4" => "D03-D04"
      case "D5-D7" => "D05-D07"
      case "D14" => "D14"
      case "D28" => "D28"
      case "D60" => "D60"
      case "HV" => "D00"
      case _=>null
    }
  }




  def getStatsOfEachSubGroup(df:DataFrame,groupName:String):DataFrame={

    val subGroup=df.filter(df("Subgroup")===groupName)

    val subGroupPatientRows=subGroup.groupBy("Patient").count().select("count").distinct().orderBy(asc("count"))
    println(s"**************************** All possible patient rows of sub group ${groupName}*******************")
    subGroupPatientRows.show()


    println(s"**************************** All possible time point of sub group ${groupName}*******************")

    val subGroupTimePoint=subGroup.select("TP_Class").distinct().orderBy(asc("TP_Class"))
    subGroupTimePoint.show(10)

    return subGroup
  }

  def WriteDataToDisk(df:DataFrame,outputPath:String,fileName:String): Unit ={
    df.coalesce(1).write.mode(SaveMode.Overwrite)
      .option("header","true")
      .option("mapreduce.fileoutputcommitter.marksuccessfuljobs","false") //Avoid creating of crc files
      .option("encoding", "UTF-8")
      .option("delimiter", outputCsvDelimiter) // set tab as delimiter, required by tranSMART
      .csv(outputPath+"/"+fileName)
  }

  def NormalizeColNameForTransmart(df:DataFrame,colNames:Array[String]):DataFrame={
    val spark=df.sparkSession
    import spark.implicits._
    //Add STUDY id
    val dfWithStudyID=df.withColumn("STUDY_ID",lit(studyID))
    //change Patient to SUBJ_ID
    val dfWithSub=dfWithStudyID.withColumnRenamed("Patient",subjID)
    val colNameWithOrder=Array("STUDY_ID",subjID)++colNames.filter(!_.equals(patientIdColName))
    val result=dfWithSub.select(colNameWithOrder.head,colNameWithOrder.tail:_*)
    return result
  }

  def ChangeColName(df:DataFrame,nameMap:Map[String,String]):DataFrame={
    val oldNames=nameMap.keySet.toArray
    var result=df
    for(oldName<-oldNames){
      result=result.withColumnRenamed(oldName,nameMap.getOrElse(oldName,"No_keys"))
    }
    return result
  }

  def countNullValue(df:DataFrame):Unit={
    val spark=df.sparkSession
    import spark.implicits._
    for(colName<-df.columns){
      val nullCount=df.filter(df(colName).isNull||df(colName).isNaN||df(colName)===""||df(colName)===nullValue).count()
      println(s"The null value count of the column $colName is $nullCount")
    }
  }

  def fillTransmartNullForDigitCol(rawDf:DataFrame,colNames:Array[String],userDefinedNull:String):DataFrame={
    val digitNull="."
    /*Step 0 : cast all column to string*/
    val df=rawDf.select(rawDf.columns.map(c=>col(c).cast(StringType)):_*)
    //df.show(5)
    /*Step 1 : fill na with digitNull to the given column*/
    val naFill=df.na.fill(digitNull,colNames)
    //naFill.show(5)
    /*Step 2: fill user defined null with digitNull*/
    val result=replaceSpecValue(naFill,colNames,userDefinedNull,digitNull)
    result
  }

  def fillTransmartNullForStrCol(rawDf:DataFrame,colNames:Array[String],userDefinedNull:String):DataFrame={
    val strNull="Not Available"
    /*Step 0 : cast all column to string*/
    val df=rawDf.select(rawDf.columns.map(c=>col(c).cast(StringType)):_*)
    // df.show(5)
    /*Step 1 : fill na with digitNull to the given column*/
    val naFill=df.na.fill(strNull,colNames)
    // naFill.show(5)
    /*Step 2: fill user defined null with digitNull*/
    val result=replaceSpecValue(naFill,colNames,userDefinedNull,strNull)
    result
  }

  def replaceSpecValue(rawDf:DataFrame,colNames:Array[String],specValue:String,newValue:String):DataFrame={
    /*Step 0 : cast all column to string*/
    val spark=rawDf.sparkSession
    import spark.implicits._
    val df=rawDf.select(rawDf.columns.map(c=>col(c).cast(StringType)):_*)

    /*Step 1 : transform spec value to null*/
    var result=df
    for(colName<-colNames){
      val newColName=colName+"_tmp"
      result=result.withColumn(newColName, when(result(colName) === specValue, newValue).otherwise(result(colName))) //create a tmp col with digitnull
        .drop(colName) //drop the old column
        .withColumnRenamed(newColName,colName) // rename the tmp to colName
    }
    result
  }

  def getDistinctValueOfColumns(df:DataFrame,colNames:Array[String],showRange:Int):Unit={

    for(colName<-colNames){
      df.select(colName).distinct().show(showRange,false)
    }
  }

  def removeRowsWithSpecValues(df:DataFrame,colName:String,specValues:Array[String]):DataFrame={
    var result=df
    for(specValue<-specValues){
      result=result.filter(!(result(colName)===specValue))
    }
    result
  }

  def getColumnNumNameMapping(df:DataFrame):scala.collection.immutable.ListMap[Int,String]={
    val columns=df.columns
    var  i=1
    var colNameNumMap=Map[Int,String]()
    for(col<-columns){
      colNameNumMap+=(i->col)
      i=i+1
    }

    ListMap(colNameNumMap.toSeq.sortWith(_._1 < _._1):_*)
  }

  /***********************************************************************************************************
    * ************************************** Annexe *******************************************
    * ******************************************************************************************************/
  /*
  *
  * the :_* syntax which means "treat this sequence as a sequence"! Otherwise, your sequence of n items will be
  * treated as a sequence of 1 item (which will be your sequence of n items).
  *
  * val seq = List(1, 2, 3)
  funcWhichTakesSeq(seq)      //1: Array(List(1, 2, 3)) -i.e. a Seq with one entry
  funcWhichTakesSeq(seq: _*)  //3: List(1, 2, 3)
  * def funcWhichTakesSeq(seq: Any*) = println(seq.length + ": " + seq)
  * */

}
