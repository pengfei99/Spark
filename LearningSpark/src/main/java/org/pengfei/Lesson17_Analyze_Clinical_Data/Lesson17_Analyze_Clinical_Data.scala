package org.pengfei.Lesson17_Analyze_Clinical_Data

import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

import scala.collection.immutable.ListMap

object Lesson17_Analyze_Clinical_Data {

  /************************************************************************************************************
    * ***************************** 17.1 Introduction *********************************************************
    * *********************************************************************************************************/
  /* In this Lesson, we will learn how to transform a dataset into a specific format. The raw dataset is provided by
   * a hospital study, we need to transform it with a specific format and load it into a bio data warehouse named
   * transmart. Transmart is not a real data warehouse in a computer scientist opinion but its close enough.
   * In this lesson, we will learn:
   * - 2. read data from excel
   * - 3. preliminary analyze on raw data
   * - 4. build new columns based on duplicate rows,
   * - 5. deal with duplicates rows/null values/change column names
   * - 6. Merge columns
   * - 7. Joining data
   * - 8. Compare two columns if they have the same value for each row
   * - 9. Other helping function
   * */

  /******************************************* Configuration ***************************************/

  val csvFile="/DATA/data_set/spark/basics/Lesson17_Analyse_Clinical_Data/raw_data.csv"
  val outputPath="/DATA/data_set/spark/basics/Lesson17_Analyse_Clinical_Data"
  val nullValue="null"
  val timePointColName="Time_Point"
  val patientIdColName="Patient"
  val separator="_"
  val v1="v1"
  val v2="v2"

  //config for output csv to match with transmart requirements
  val studyID="Realism01"
  val subjID="SUBJ_ID"
  val outputCsvDelimiter="\t"

  def main(args:Array[String]):Unit= {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val spark = SparkSession.builder().appName("Lesson17_Analyze_Clinical_Data").master("local[2]").getOrCreate()
    import spark.implicits._

    /************************************************************************************************************
      * ***************************** 17.2 Read data from excel *********************************************************
      * *********************************************************************************************************/

      /* Here we use a 3rd party lib to read excel data, you can find the maven dependence
      * <dependency>
            <groupId>com.crealytics</groupId>
            <artifactId>spark-excel_2.11</artifactId>
            <version>0.10.1</version>
        </dependency>
      *
      * But we have problem with encodings, the excel by default does not use utf8 as encoding, so the special character
      * will not be printed normally. To resolve this encoding problem, we use lib-office to export a csv with utf-8
      * */

    /*val filePath = "/DATA/data_set/spark/basics/Lesson17_Analyse_Clinical_Data/raw_data.xlsx"

    val df = spark.read
      .format("com.crealytics.spark.excel")
      .option("sheetName", "workable_long_missing_type") // Required
      .option("useHeader", "true") // Required
      .option("treatEmptyValuesAsNulls", "true") // Optional, default: true
      .option("inferSchema", "true") // Optional, default: false
      .load(filePath)*/

   /* val filePath = "/DATA/data_set/spark/basics/Lesson17_Analyse_Clinical_Data/sample.xlsx"

    val df = spark.read
      .format("com.crealytics.spark.excel")
      .option("sheetName", "Feuil1") // Required
      .option("useHeader", "true") // Required
      .option("treatEmptyValuesAsNulls", "true") // Optional, default: true
      .option("inferSchema", "true") // Optional, default: false
      .load(filePath)

    df.show(1)
    val columNum=df.columns.length

    println(s"columNum is $columNum")
    */





   /******************************************************************************************************
     * ************************************17.3 Preliminary analyze on raw data *****************************
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


    /***********************************************************************************************************
      * ****************** 17.4 Build new columns based on duplicate rows of patients **************************
      * ******************************************************************************************************/

      /*************************************17.4.1 Change date value for easier sorting *************************/
    /* We noticed that with the current time point column, when we do sorting, the value does not sort in good order
    *  So we need to change the value as shown below
    * */
    /*
| D0->Day 00|
| D1->Day 01|
| D1-D2->Day 01-Day 02|
| D14->Day 14|
| D2->Day 02|
| D28->Day 28|
| D3-D4->Day 03-Day 04|
| D5-D7->Day 05-Day 07|
| D60->Day 60|
| HV->Day 00|
  * */
/* The detail of the implementation is encapsulate in the function ModifyTimePoint */
  val dfWithTP= ModifyTimePoint(csvDF)

    /************************ 17.4.2 Build column Based on the patient time point row ***********************/

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
      * ************************** 17.4.3 SOFA time point related data treatment ***************************
      * ******************************************************************************************************/

    /* SOFA data is in scenario 1 */
    val sofaValueColumns=Array("CBD_Cardio_SOFA","CBD_Coag_SOFA",
      "CBD_Dobut_SOFA","CBD_Hepat_SOFA",
      "CBD_Neuro_SOFA","CBD_Renal_SOFA",
      "CBD_Resp_SOFA","CBD_SOFA_NA","CBD_SOFA")
    val utilityColumns=Array(patientIdColName,timePointColName)


    /*
    // build a small test dataset to test the correctness of the function
    val sofaValueColumns=Array("CBD_Cardio_SOFA","CBD_Coag_SOFA")
    val patient1088=dfWithTP.filter($"Patient"===1088)
    val sofaTest=BuildColumnsWithTimePointS1(patient1088,sofaValueColumns,utilityColumns)
    val sofaTestRowNum=sofaTest.count()
    val sofaTestColNum=sofaTest.columns.length
    println(s"sofa row number is ${sofaTestRowNum}, sofa column number is ${sofaTestColNum}")
    */

    /* The implementation of how we transform duplicate rows into columns is in function BuildColumnsWithTimePointS1,
    * and this function is strongly depends on function rowToColumn. The rowToColumn is the core function which can
    * transform duplicate rows of a column into multiple columns.*/

    /*
val sofaTPData=BuildColumnsWithTimePointS1(dfWithTP,sofaValueColumns,utilityColumns)
val sofaRowNum=sofaTPData.count()
val sofaColNum=sofaTPData.columns.length*/

// sofa row number is 552, sofa column number is 82
// println(s"sofa row number is ${sofaRowNum}, sofa column number is ${sofaColNum}")






/***********************************************************************************************************
  * ************************** 17.4.4 BioMarker time point related data treatment ***************************
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

/* Test with Cytometry Tcells dataset*/

/*Step 0: build columns with Time point */
val tCellsCols= Array("Treg_Percentage","Treg_cells_per_µl","T_cells_percentage","T_cells_per_µl","T4_cells_percentage",
  "T4_cells_per_µl","T8_cells_percentage","T8_cells_per_µl","Ratio_T4_T8_percentage","T4_T8_cells_percentage")

    val fullSortedTCellsCols=generateFullCols("FLOW_CYTOMETRY",tCellsCols)
    val dfCyto=dfWithTP.filter(col("Plateform")==="FLOW_CYTOMETRY")
    val dfTCells=dfCyto.filter(col("Marker").isin(tCellsCols: _*))

    val tCellsTPRawData=BuildColumnsWithTimePointS2(dfTCells)

    tCellsTPRawData.count()
    tCellsTPRawData.columns.size

    /* Step 1: normalize data for transmart format*/

    val tCellsData=NormalizeColNameForTransmart(tCellsTPRawData, fullSortedTCellsCols)

    //neutroData.show(5)

    /*Step2 : change col name*/

    /*Step3 : check null cell count for each column*/

    /* Step 4 : fill null value with transmart required value*/

    /* Step 5: Write data to disk*/
    // WriteDataToDisk(tCellsData,"hdfs://hadoop-nn.bioaster.org:9000/realism/output","FC_TCells_V3")

    /* Step 6 : Get column name number mapping */
    val tCellsColNameNumMapping=getColumnNumNameMapping(tCellsData)

    tCellsColNameNumMapping.foreach(println)

/*
// The implementation of bioMarker transformation is done in BuildColumnsWithTimePointS2, it's also strongly depends on
// the rowToColumn function
val bioMarkerWithTPColData=BuildColumnsWithTimePointS2(dfWithTP)
bioMarkerWithTPColData.show(5,false)
val rowNum=bioMarkerWithTPColData.count()
val colNum=bioMarkerWithTPColData.columns.length
// The row number is 552, the column number is 1270, the row number is 552 which is correct of the total patient number
println(s"The row number is ${rowNum}, the column number is ${colNum}")
*/

    /*************************************************************************************************************
      ************* 17.5 Deal with null value cell/ remove duplicate columns/ change column name ******************
      ********************************************************************************************************/

    /* To see how we deal with null value, duplicate columns, change column name, we use a sub-dataframe which includes
    * all demographic data of the patient.
    *
    * The implementation is done in the function ExportDemographicData. This function calls:
    * - changeColName
    * - countNullValue
    * - fillTransmartNullForDigitCol
    * - fillTransmartNullForStrCol
    * - replaceSpecValue
    *
    * */

    // ExportDemographicData(csvDF)

/***********************************************************************************************************
  * ******************************************* 17.6 Merge column ******************************************
  * ******************************************************************************************************/

    /* As we explained before, the raw data is built based on two study version, v1 does not support time point, v2
     * supports the time point, The v1 column name contains Theoretical_D1 (e.g. CBD_Cardio_SOFA_Theoretical_D1). The
     * v2 column names are without Theoretical_D1 (e.g. CBD_Cardio_SOFA). There is another column which indicates the
     * time point. We want to merge the v1 column with v2 of day 01 column.
     *
     * The implementation is done in function ExportMergedSofa, it can be divide into three main steps:
     * - Build sofa v1 dataframe which contains all sofa v1 columns
     * - Build sofa v2 dataframe, we need to transform the duplicated row to column with time point.
     * - Merge sofa v1 and v2 columns, this steps calls function mergeSofaColumns, which is the core function of merge.
     * */

   //val fullSofaDf=ExportMergedSofa(dfWithTP)



    /***********************************************************************************************************
      * ******************************* 17.7 Joining data  *****************************************************
      * ******************************************************************************************************/

    /*
     * As we merged the sofa score of v1 and v2, we need to build a new column for indicating the data's study version
      *  The REALISM_patient_list.csv contains two column, Patient v1 column contains all patient id of version1.
     * Patient v2 column contains all patient id of version 2. */
val patientListDf=spark.read.option("inferSchema", true).option("header",true)
      .option("nullValue"," ")
      .option("encoding", "UTF-8")
      .option("delimiter",",")
      .csv("/DATA/data_set/spark/basics/Lesson17_Analyse_Clinical_Data/REALISM_patient_list.csv")

    //patientListDf.show(5)


    ExportPartientStudyVersion(patientListDf,csvDF)

    /***********************************************************************************************************
      * ******************************************* 17.8 Compare two columns ********************************
      * ******************************************************************************************************/


    /***********************************************************************************************************
      * ******************************************* 17.9 Other helping function ********************************
      * ******************************************************************************************************/

    /* We also developed a few helping function which can the export to transmart much easier. You can find them
    * in the following list:
    * - NormalizedColNameForTransmart
    * - getDistinctValueOfColumns (reusable in any dataframe)
    * - removeRowsWithSpecValues (reusable in any dataframe)
    * - getColumnNumNameMapping (reusable in any dataframe)
    * - WriteDataToDisk
    * - GetStatsOfEachSubGroup
    *
    * The function with Capital letter have dependencies(e.g. column names) with this data, so it can't be used in
    * other data directly.
    * */

}


  /**
    * This function get the patient study version of Realism, if the given column belongs to v2, patient of v1 must
    * have value null, if not null, patient belongs v2
    * @author Pengfei liu
    * @version 1.0
    * @since 2019-02-13
    * @param patientListDf patientListDf is a  dataframe built from REALISM_patient_list.csv
    * @param df df is the main source dataframe where we get all the study data
    * @return DataFrame
    * */
/** patientListDf , df is  */
  def ExportPartientStudyVersion(patientListDf:DataFrame,df:DataFrame):DataFrame={
    /* Step1: build a dataframe for Patient v1 which contains patientId, and study_version which contains value only
    * for patient of v1,  */
    // select column Patient v1 and drop null rows
    val patientV1=patientListDf.select("Patient v1").distinct().na.drop()
    //val patient_v1_count=patientV1.count()
    val patientV1Df=patientV1.withColumnRenamed("Patient v1","Patient")
      .withColumn("Study_version",lit(v1))


    /* Step2: build a dataframe for Patient v2 which contains patientId, and study_version which contains value only
    * for patient of v2,  */
    val patientV2=patientListDf.select("Patient v2").distinct().na.drop()
    val patientV2Df=patientV2.withColumnRenamed("Patient v2","Patient")
      .withColumn("Study_version",lit(v2))

    //val patient_v2_count=patientV2.count()
   // println(s"V1 has ${patient_v1_count} patients, V2 has ${patient_v2_count} patients, In total: ${patient_v1_count+patient_v2_count}")

    /* Step3: Union df p1 and p2,*/
    val patientVersionFullDf=patientV1Df.union(patientV2Df)

    /* Step4: build a dataframe for subgroup*/
    val patientSubGroupDf=df.select("Patient","Subgroup").dropDuplicates();

    /* Step5: Join the two dataframe*/
    val patientVersionGroupDf=patientSubGroupDf.join(patientVersionFullDf,Seq(patientIdColName),joinType = "inner")

    patientVersionGroupDf.show(5,false)

    /* Step6: count null value*/
    //countNullValue(patientVersionGroupDf)

    /* Step7: replace null*/
    /* no null value found */

    /* Step8: normalize data for transmart format */
    val versionGroupColumns=Array("Subgroup","Study_version");
    val patientVersionGroupFinalDf=NormalizeColNameForTransmart(patientVersionGroupDf,versionGroupColumns)
    patientVersionGroupFinalDf.show(5,false)
    /* Step9: output data to disk */

    WriteDataToDisk(patientVersionGroupFinalDf,"/tmp/Realism","version_group_data")

    /************************* Annexe ********************/
    /* We find out that, the patient list file has 553 patient, and the source data file has 552 patient. The following
    * code find out which patient in patient list is not present in the source data file. The patient 1006 refused to
    * publish his data, so he is in the patient list but not in the source data file. So we need to remove it from
    * patientVersionFullDf*/
    /*val patientSource=df.select("Patient")
    val patientNotInSource=patientVersionFullDf.select("Patient").except(patientSource)
    patientNotInSource.show()*/
return df
  }





  /*************************************************************************************************************
    ************************************ 17.4 Function Implementation *************************************
    **********************************************************************************************************/

  /*********************************** 17.4.1 Change date value for easier sorting *******************************/

  def ModifyTimePoint(df:DataFrame):DataFrame={
    val spark=df.sparkSession
    spark.udf.register("changeTimePoint",(timePoint:String)=>changeTimePoint(timePoint))
    val dfWithNewTimePoint=df.withColumn("Time_Point",expr("changeTimePoint(TP_Class)"))
    dfWithNewTimePoint.select("TP_Class","Time_Point").distinct().show(10)

    /*
    //The following code write the result dataframe on disk
    dfWithNewTimePoint.coalesce(1).write.mode(SaveMode.Overwrite)
    .option("header","true")
    .option("mapreduce.fileoutputcommitter.marksuccessfuljobs","false") //Avoid creating of crc files
    .option("encoding", "UTF-8")
    .csv(outputPath+"/TimePoint")
    */
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
      case _=>"null"
    }
  }

  /*********************************** 17.4.3 SOFA time point related data treatment *******************************/
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

  /****************** 17.4.4 BioMarker time point related data treatment *******************/

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



    /*
    //check the bioMarker data schema and if data contains many null
    bioMarkerData.printSchema()
    val allValue=bioMarkerData.count()
    val nonNullValue=bioMarkerData.filter($"Value".isNotNull).count()
    println(s"All value count is ${allValue}, nonNullValue count is ${nonNullValue}")
    */



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

  /*************************************17.4 Core function rowToColumn *******************************************/
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
      // the reduce(_ ++ _) translates to reduce((a,b)=>a++b) where a, b are lists, ++ is a method in list interface
      // which concatenates list b to a.
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



  /***********************************************************************************************************
    * *************** 17.5 Deal with null value cell/ duplicate columns/ change column name  **********************
    * ******************************************************************************************************/

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


    /* Step 4 : fill null value with transmart required value (. for digit, Not Available for string)*/
    /* We know Height, Weight, BMI are all digit columns, so we replace them with .
    * */
    val demoFinalData=fillTransmartNullForDigitCol(demoRenamedDf,Array("Height","Weight","BMI"),nullValue)
    countNullValue(demoFinalData)

    /* Step 5 : output data to disk */
    WriteDataToDisk(demoFinalData,"/tmp/Realism","demographic_data")

  }

  /**
    * This function takes a data frame,and a Map[oldColName,newColName], it will replace the old column name by the
    * new column name and returns the data frame with new names.
    *
    * @author Pengfei liu
    * @version 1.0
    * @since 2018-12-20
    * @param df The source data frame.
    * @param nameMap A Map of [oldColName,newColName]
    * @return DataFrame
    * */
  def ChangeColName(df:DataFrame,nameMap:Map[String,String]):DataFrame={
    val oldNames=nameMap.keySet.toArray
    var result=df
    for(oldName<-oldNames){
      result=result.withColumnRenamed(oldName,nameMap.getOrElse(oldName,"No_keys"))
    }
    return result
  }

  /**
    * This function takes a data frame, it prints null value counts of all columns of the data frame
    *
    * @author Pengfei liu
    * @version 1.0
    * @since 2018-12-20
    * @param rawDf The source data frame.
    * @return Unit
    * */
  def countNullValue(df:DataFrame):Unit={
    val spark=df.sparkSession
    import spark.implicits._
    for(colName<-df.columns){
      val nullCount=df.filter(df(colName).isNull||df(colName).isNaN||df(colName)===""||df(colName)===nullValue).count()
      println(s"The null value count of the column $colName is $nullCount")
    }
  }
  /**
    * This function takes a data frame, a list of column names, and a user defined null value, it will replace the
    * default null (df.na) and user define null value in the data frame by the transmart digit null value in all
    * given columns of the data frame.
    *
    * @author Pengfei liu
    * @version 1.0
    * @since 2018-12-20
    * @param rawDf The source data frame.
    * @param colNames A list of column names
    * @param userDefinedNull A string value which is defined by user in the data frame to represent null.
    * @return DataFrame
    * */
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
  /**
    * This function takes a data frame, a list of column names, and a user defined null value, it will replace the
    * default null (df.na) and user define null value in the data frame by the transmart String null value in all
    * given columns of the data frame.
    *
    * @author Pengfei liu
    * @version 1.0
    * @since 2018-12-20
    * @param rawDf The source data frame.
    * @param colNames A list of column names
    * @param userDefinedNull A string value which is defined by user in the data frame to represent null.
    * @return DataFrame
    * */
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
  /**
    * This function takes a data frame, a list of column names, a old value, and a new value, it will replace the old
    * value by the new value in all given columns of the data frame.
    *
    * @author Pengfei liu
    * @version 1.0
    * @since 2018-12-20
    * @param df The source data frame.
    * @param colNames A list of column names
    * @param specValue A string value which needs to be replaced
    * @param newValue A string value which will repalce the old value
    * @return DataFrame
    * */
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


  /******************************************* 17.6 Merge column  ******************************************/


  def ExportMergedSofa(df:DataFrame):DataFrame={
    val spark=df.sparkSession;
    import spark.implicits._
    /***************************1. get Sofa v1 column *********************************/
    val sofaD1=Array("Patient","CBD_Cardio_SOFA_Theoretical_D1","CBD_Coag_SOFA_Theoretical_D1",
      "CBD_Dobut_SOFA_Theoretical_D1","CBD_Hepat_SOFA_Theoretical_D1","CBD_Neuro_SOFA_Theoretical_D1","CBD_Renal_SOFA_Theoretical_D1",
      "CBD_Resp_SOFA_Theoretical_D1","CBD_SOFA_Theoretical_D1")


    val sofaD1Data=df.select(sofaD1.head,sofaD1.tail:_*).dropDuplicates().orderBy($"Patient".asc)

    /* Step0: to avoid null pointer exception, cast all columns to string and fill na with "null"*/
    val sofaD1StrData = sofaD1Data.select(sofaD1Data.columns.map(c => col(c).cast(StringType)) : _*).na.fill(nullValue)
    /*Step1: normalize data for transmart format*/
    // We drop the column STUDY_ID to avoid duplicate column after merge with the v2 column
    val sofaD1NormData=NormalizeColNameForTransmart(sofaD1StrData,sofaD1).drop("STUDY_ID")
    sofaD1NormData.show(3,false)

    /*Step2 : change col name*/

    /*Step3 : check null col*/
    //countNullValue(sofaD1NormData)

    /***************************2. get Sofa v2 column *********************************/
    val sofaValueColumns=Array("CBD_Cardio_SOFA","CBD_Coag_SOFA",
      "CBD_Dobut_SOFA","CBD_Hepat_SOFA",
      "CBD_Neuro_SOFA","CBD_Renal_SOFA",
      "CBD_Resp_SOFA","CBD_SOFA_NA","CBD_SOFA")
    val utilityColumns=Array(patientIdColName,timePointColName)
    val allColumns=utilityColumns++sofaValueColumns
    /* Step0: pre-process data */
    val sofaTPRawData=df.select(allColumns.head,allColumns.tail:_*).dropDuplicates().orderBy($"Patient".asc)
    /* We can conclude the refine process is correct, we have 981 null rows in D14, D28 and D60, before refine process,
    * we have 2452 rows, after we have 1471 rows */
    val sofaTPRefinedData=removeRowsWithSpecValues(sofaTPRawData,"Time_Point",Array("D14","D28","D60"))

    sofaTPRefinedData.show(3,false)

    sofaTPRefinedData.cache()
    /*Step1: transform multi rows to columns*/
    val sofaTPData=BuildColumnsWithTimePointS1(sofaTPRefinedData,sofaValueColumns,utilityColumns)

    /*Step2: normalize data for transmart format*/
    val sofaTPNormData=NormalizeColNameForTransmart(sofaTPData,sofaTPData.columns.toArray)
    sofaTPNormData.show(3,false)

    /*Step3 : change col name*/

    /*Step4 : check null col, all value columns has null values, so we need to do
    * fill null on all columns */
    //countNullValue(sofaTPNormData)

    /***************************3. Merge the v1 and v2 column ************************/


    /* We choose to merge V1 columns with V2 Day 01 columns, To do this we need to call function mergeSofaColumn which
     * can merge all elements in the sofaValueColumns, except "CBD_SOFA_NA", because it only exist for V2.*/
    /*Step 1. join v1 and v2 dataframe */
    val sofaFullDf = sofaTPNormData.join(sofaD1NormData, Seq("SUBJ_ID"))
    /*Step 2. merge v1 and v2 columns*/
    val mergedSofaDf=mergeSofaColumns(sofaFullDf,sofaValueColumns)

    /***************************3. Normalize column order ************************/
    /* To facilitate the usage, we sort the column with a given order*/
    val sofaOrderedCol=Array( "STUDY_ID","SUBJ_ID",
      "CBD_Cardio_SOFA_D00","CBD_Cardio_SOFA_D01","CBD_Cardio_SOFA_D01-D02","CBD_Cardio_SOFA_D02","CBD_Cardio_SOFA_D03-D04","CBD_Cardio_SOFA_D05-D07",
      "CBD_Coag_SOFA_D00","CBD_Coag_SOFA_D01","CBD_Coag_SOFA_D01-D02","CBD_Coag_SOFA_D02","CBD_Coag_SOFA_D03-D04","CBD_Coag_SOFA_D05-D07",
      "CBD_Dobut_SOFA_D00","CBD_Dobut_SOFA_D01","CBD_Dobut_SOFA_D01-D02","CBD_Dobut_SOFA_D02","CBD_Dobut_SOFA_D03-D04","CBD_Dobut_SOFA_D05-D07",
      "CBD_Hepat_SOFA_D00","CBD_Hepat_SOFA_D01","CBD_Hepat_SOFA_D01-D02","CBD_Hepat_SOFA_D02","CBD_Hepat_SOFA_D03-D04","CBD_Hepat_SOFA_D05-D07",
      "CBD_Neuro_SOFA_D00","CBD_Neuro_SOFA_D01","CBD_Neuro_SOFA_D01-D02","CBD_Neuro_SOFA_D02","CBD_Neuro_SOFA_D03-D04","CBD_Neuro_SOFA_D05-D07",
      "CBD_Renal_SOFA_D00","CBD_Renal_SOFA_D01","CBD_Renal_SOFA_D01-D02","CBD_Renal_SOFA_D02","CBD_Renal_SOFA_D03-D04","CBD_Renal_SOFA_D05-D07",
      "CBD_Resp_SOFA_D00","CBD_Resp_SOFA_D01","CBD_Resp_SOFA_D01-D02","CBD_Resp_SOFA_D02","CBD_Resp_SOFA_D03-D04","CBD_Resp_SOFA_D05-D07",
      "CBD_SOFA_NA_D00","CBD_SOFA_NA_D01","CBD_SOFA_NA_D01-D02","CBD_SOFA_NA_D02","CBD_SOFA_NA_D03-D04","CBD_SOFA_NA_D05-D07",
      "CBD_SOFA_D00","CBD_SOFA_D01","CBD_SOFA_D01-D02","CBD_SOFA_D02","CBD_SOFA_D03-D04","CBD_SOFA_D05-D07")
    val sofaMergedAndOrderedDf=mergedSofaDf.select(sofaOrderedCol.head,sofaOrderedCol.tail:_*)

    sofaMergedAndOrderedDf.show(2,false)

    // write to disk, this version is for sanofi, all null value are "null"
    WriteDataToDisk(sofaMergedAndOrderedDf,"/tmp/Realism","SofaTP_data_sanofi")

    /************************4. replace null with transmart required null value ********/

    /*/* string columns*/

    val strColumns=Array("CBD_Dobut_SOFA_D00","CBD_Dobut_SOFA_D01","CBD_Dobut_SOFA_D01-D02","CBD_Dobut_SOFA_D02","CBD_Dobut_SOFA_D03-D04","CBD_Dobut_SOFA_D05-D07",
      "CBD_SOFA_NA_D00","CBD_SOFA_NA_D01","CBD_SOFA_NA_D01-D02","CBD_SOFA_NA_D02","CBD_SOFA_NA_D03-D04","CBD_SOFA_NA_D05-D07")
    val fillStr=fillTransmartNullForStrCol(mergedSofaDf,strColumns,nullValue)
    /* digit columns*/
    val digitColumns=Array("CBD_Cardio_SOFA_D00","CBD_Cardio_SOFA_D01","CBD_Cardio_SOFA_D01-D02","CBD_Cardio_SOFA_D02","CBD_Cardio_SOFA_D03-D04","CBD_Cardio_SOFA_D05-D07",
      "CBD_Coag_SOFA_D00","CBD_Coag_SOFA_D01","CBD_Coag_SOFA_D01-D02","CBD_Coag_SOFA_D02","CBD_Coag_SOFA_D03-D04","CBD_Coag_SOFA_D05-D07",
      "CBD_Hepat_SOFA_D00","CBD_Hepat_SOFA_D01","CBD_Hepat_SOFA_D01-D02","CBD_Hepat_SOFA_D02","CBD_Hepat_SOFA_D03-D04","CBD_Hepat_SOFA_D05-D07",
      "CBD_Neuro_SOFA_D00","CBD_Neuro_SOFA_D01","CBD_Neuro_SOFA_D01-D02","CBD_Neuro_SOFA_D02","CBD_Neuro_SOFA_D03-D04","CBD_Neuro_SOFA_D05-D07",
      "CBD_Renal_SOFA_D00","CBD_Renal_SOFA_D01","CBD_Renal_SOFA_D01-D02","CBD_Renal_SOFA_D02","CBD_Renal_SOFA_D03-D04","CBD_Renal_SOFA_D05-D07",
      "CBD_Resp_SOFA_D00","CBD_Resp_SOFA_D01","CBD_Resp_SOFA_D01-D02","CBD_Resp_SOFA_D02","CBD_Resp_SOFA_D03-D04","CBD_Resp_SOFA_D05-D07",
      "CBD_SOFA_D00","CBD_SOFA_D01","CBD_SOFA_D01-D02","CBD_SOFA_D02","CBD_SOFA_D03-D04","CBD_SOFA_D05-D07")

    val fillDigit=fillTransmartNullForDigitCol(fillStr,digitColumns,nullValue)

    val finalSofaTPData=fillDigit
    /*Step5 : output data to disk*/
    WriteDataToDisk(finalSofaTPData,"/tmp/Realism","SofaTP_data")*/

    return sofaMergedAndOrderedDf
  }

  /**
    * This function merges values of two column, if one is null, return other, if two values are not null, check if they
    * are equal, otherwise raise exception, two column cant be merged.
    * @author Pengfei liu
    * @version 1.0
    * @since 2019-02-13
    * @param col1Value first column value to be merged
    * @param col2Value second column value to be merged
    * @return String
    * */
  def mergeValue(col1Value:String,col2Value:String):String={

    if (col1Value.equals("null")) {return col2Value}
    else if(col2Value.equals("null") || col1Value.equals(col2Value)) {return col1Value}
    else {return "error"}

  }

  // define a spark udf for mergeValue funciton
  val mergeValueUDF = udf(mergeValue(_:String, _: String))

  /**
    * This function takes a dataframe and a list of sofa v2 column names. Based on the v2 column names, it can build the
    * corresponding v1 column names, then it calls the udf mergeValue to merge the v1 and v2 column. In the end it
    * removes the v2 day01 and v1 column, and add the merged column.
    *
    * @author Pengfei liu
    * @version 1.0
    * @since 2019-02-27
    * @param df the source data frame
    * @param colNameList the sofa v2 column names list
    * @return DataFrame
    * */
  def mergeSofaColumns(df:DataFrame,colNameList:Array[String]):DataFrame={

    var result=df
    for(colName<-colNameList){
      // We exclude CBD_SOFA_NA, because it does not exist in V1, so no need to do the merge
      if (!colName.equals("CBD_SOFA_NA")){
        /* CBD_Cardio_SOFA, generates CBD_Cardio_SOFA_D01 and CBD_Cardio_SOFA_Theoretical_D1 */
        val col1Name=s"${colName}_Theoretical_D1"
        val col2Name=s"${colName}_D01"
        result=result.withColumn(s"merged_${col2Name}", mergeValueUDF(col(col1Name),col(col2Name)))
        //check the merge result
        result.select(s"merged_${col2Name}",col1Name,col2Name).show(10,false)
        //clean the result, drop v1 and V2 day01 columns, and rename merged_column to V2 day01
        result=result.drop(col1Name)
          .drop(col2Name)
          .withColumnRenamed(s"merged_${col2Name}",col2Name)
      }

    }
    result
  }

  /******************************************** 17.7 Joining data *******************************************/

  /******************************************** 17.8 Compare two columns *************************************/

  /******************************************** 17.9 Other helping function ************************************/

  /* This function add a new STUDY_ID column, rename the Patient column to subjID*/
  def NormalizeColNameForTransmart(df:DataFrame,colNames:Array[String]):DataFrame={
    val spark=df.sparkSession
    import spark.implicits._
    /* step 0: cast all column to string*/
    val dfStr=df.select(df.columns.map(c=>col(c).cast(StringType)):_*)

    /* step 1: fill na with user defined null*/
    val dfNaFill=dfStr.na.fill(nullValue,dfStr.columns)

    /* step 2: add column study_id */
    val dfWithStudyID=dfNaFill.withColumn("STUDY_ID",lit(studyID))
    /* step 3: change col name Patient to SUBJ_ID*/
    val dfWithSub=dfWithStudyID.withColumnRenamed("Patient",subjID)
    /* step 4: Re-order columns*/
    val colNameWithOrder=Array("STUDY_ID",subjID)++colNames.filter(!_.equals(patientIdColName))
    val result=dfWithSub.select(colNameWithOrder.head,colNameWithOrder.tail:_*)
    return result
  }

  /**
    * This function takes a data frame and a list of column names, it will print the distinct value of each given column
    *
    * @author Pengfei liu
    * @version 1.0
    * @since 2018-12-20
    * @param df The source data frame.
    * @param colNames A list of column names
    * @return Unit
    * */
  def getDistinctValueOfColumns(df:DataFrame,colNames:Array[String],showRange:Int):Unit={

    for(colName<-colNames){
      df.select(colName).distinct().show(showRange,false)
    }
  }

  /**
    * This function takes a data frame, a column name and an array of specific value. It will remove all rows if the
    * given column contains the specific value in the Array.
    *
    * @author Pengfei liu
    * @version 1.0
    * @since 2018-12-20
    * @param df The source data frame.
    * @param colName target column name
    * @param specValues an Array of specific values
    * @return DataFrame
    * */
  def removeRowsWithSpecValues(df:DataFrame,colName:String,specValues:Array[String]):DataFrame={
    var result=df
    for(specValue<-specValues){
      result=result.filter(!(result(colName)===specValue))
    }
    result
  }

  /**
    * This function takes a data frame and returns a map of (colNum->colName), the elements of the return map are
    * sorted by the column number with asc order.
    *
    * @author Pengfei liu
    * @version 1.0
    * @since 2018-12-20
    * @param df The source data frame.
    * @return a Map[Int, String]
    * */
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

  /* This function write the input dataframe to the output file system*/
  def WriteDataToDisk(df:DataFrame,outputPath:String,fileName:String): Unit ={
    df.coalesce(1).write.mode(SaveMode.Overwrite)
      .option("header","true")
      .option("mapreduce.fileoutputcommitter.marksuccessfuljobs","false") //Avoid creating of crc files
      .option("encoding", "UTF-8")
      .option("delimiter", outputCsvDelimiter) // set tab as delimiter, required by tranSMART
      .csv(outputPath+"/"+fileName)
  }



  /* This study has five sub group of patients	HV(Healthy Volunteer), Septic shock, Trauma, Surgery, Burn. This function
  * get info such as row number, all possible time point for each sub group*/
  def GetStatsOfEachSubGroup(df:DataFrame,groupName:String):DataFrame={

    val subGroup=df.filter(df("Subgroup")===groupName)

    val subGroupPatientRows=subGroup.groupBy("Patient").count().select("count").distinct().orderBy(asc("count"))
    println(s"**************************** All possible patient rows of sub group ${groupName}*******************")
    subGroupPatientRows.show()


    println(s"**************************** All possible time point of sub group ${groupName}*******************")

    val subGroupTimePoint=subGroup.select("TP_Class").distinct().orderBy(asc("TP_Class"))
    subGroupTimePoint.show(10)

    return subGroup
  }

  /**
    * This function build full colName for bioMarker, flow cytometry after row to column transformation
    * @author Pengfei liu
    * @version 1.0
    * @since 2019-07-06
    * @param CategoryName The category name of the bioMarker e.g. FLOW_CYTOMETRY.
    * @param colNames colNames represent all bioMarker values which will be transformed into column names in the result
    * @return  Array[String] full list of the bioMarker resulting col name
    * */
  def generateFullCols(CategoryName:String, colNames:Array[String]):Array[String]={
    val tps=Array("D00","D01","D01-D02","D02","D03-D04","D05-D07","D14","D28","D60")
    val tails=Array("Value","Imputed_Value","Missing_Value_Type")
    var result:Array[String]=Array()
    for(colName<-colNames){
      for(tp<-tps){
        for(tail<-tails){
          val fullColName=s"${CategoryName}_${colName}_${tp}/${tail}"
          result=result:+fullColName
        }
      }
    }
    return result
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
