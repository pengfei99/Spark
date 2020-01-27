package org.pengfei.Lesson10_Spark_Application_ETL

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

object Lesson10_2_data_cleaning {

  /** ***********************************************************************************************
    * ***********************************10.2 Data cleaning with Spark ********************************
    * **********************************************************************************************/

  /*********************************10.2.1 Get basic data quality stats *********************************************/

  /* If we have many null cells in a data set, and null is not expected. We can say that data quality is low. In spark
  * we have isNull, isNotNull, isNaN to detect null values and not a number.
  *
  * Null values represents "no value" or "nothing", it's not even an empty string or zero. It can be used to represent
  * that nothing useful exists.
  *
  * NaN stands for "Not a Number", it's usually the result of a mathematical operation that doesn't make sense,
  * e.g. 0.0/0.0 returns true. "toto" returns false. As a result, we can't use it to determine if a int column
  * contains not digit value or not. We need to write our UDF to do that.
  *
  *
  * */


  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val spark = SparkSession.builder().appName("Lesson10_2_data_cleaning").master("local[2]").getOrCreate()
    import spark.implicits._

    /*  //Test of isDigit function

     val val1 = "-1.1"
     val val2 = "22"
     val val3 = "1,1"

     println(s"$val1 is digit ${isDigit(val1)}")
     println(s"$val2 is digit ${isDigit(val2)}")
     println(s"$val3 is digit ${isDigit(val3)}")*/

    val sparkConfig = ConfigFactory.load("application.conf").getConfig("spark")
    val path = sparkConfig.getString("sourceDataPath")
    val filePath = s"${path}/spark_lessons/Lesson10_Spark_Application_ETL/data_cleaning"
    val fileName = "/error1.csv"


    val error1Schema = StructType(Array(
      StructField("City", StringType, true),
      StructField("Year", IntegerType, true),
      StructField("Income", DoubleType, true)))

    // when we inferSchema, the digit column is automatically turn into integer, if you have 10.0 or space before 10.
    // The column type is inferred automatically into double.
    val error1Df = spark.read.option("header", "false").option("nullValue", "?").option("inferSchema", "true").csv(filePath + fileName).toDF("City", "Year", "Income")

    // when we give an explicit schema, everything is ok. Now try to modify the csv file, in the year column add some
    // string, and check the data frame result. You can notice, the line which contains year string value returns null
    // in all columns of the data frame

    // val error1Df=spark.read.option("header","false").schema(error1Schema).csv(filePath+fileName)
    error1Df.show()
   // error1Df.printSchema()

    /*********************************10.2.1.1 Count null cells *********************************************/

    /*
    * Null value exists almost in all data sets, We need to detect them, count them. Check the getNullCountForAllCols
    * function to see how we detect null values. The following code is an example
    * */
    val nullCountDF=getNullCountForAllCols(error1Df)
    nullCountDF.show(nullCountDF.count().toInt,false)

    /*********************************10.2.1.2 Count notDigit cells *********************************************/
      /* In a column, if most of the cells has only digit values, we should convert this columns into digit column
      * Because we have better arithmetic functions support. The cell value which is not digits in these kind of
      * column may be caused by errors. We need to detect them and count them. Sometimes, even show the value.
      *
      * Note we consider null as valid digit value.
      */

/* Check getIsDigitDF function, we use a user define function to create a new data frame, Following code is an
* example*/

    val isDigitDf = getIsDigitDF(error1Df, List("Year", "Income"))
    isDigitDf.show()

    /* With the new data frame produced, we can count how many bad values in a column, we can also show the lines
     * which contains the bad values
     * */

    val badValues=getIsNotDigitCount(isDigitDf)
    badValues.show()

    /* based on the output of badValues, we can determine column Year and Income contains*/
    showNotDigitValues(isDigitDf,List("Year","Income"))

  }

  /**
    * This function counts the null cell number
    *
    * @author Pengfei liu
    * @version 1.0
    * @since 2020-01-27
    * @param df      source data frame
    * @param colName second column value to be merged
    * @return Long, It returns the number of null cell
    **/

  def getNullCount(df: DataFrame, colName: String): Long = {
    df.select(colName).filter(col(colName).isNull).count()
  }

  /**
    * This function counts the null cell number for all columns of the source data frame
    *
    * @author Pengfei liu
    * @version 1.0
    * @since 2020-01-27
    * @param df source data frame
    * @return DataFrame, It returns a data frame which contains three columns ("column_name","null_cell_count","null_cell_percentage")
    **/
  def getNullCountForAllCols(df: DataFrame): DataFrame = {
    val spark = df.sparkSession
    val totalLineNb = df.count()
    import spark.implicits._
    val buf = scala.collection.mutable.ListBuffer.empty[(String, Long, Double)]
    for (colName <- df.columns) {
      val nulLineNb = getNullCount(df, colName)
      val nullPercentage: Double = (nulLineNb.toDouble / totalLineNb.toDouble) * 100
      buf.append((colName, nulLineNb, nullPercentage))
    }
    val result = buf.toList.toDF("column_name", "null_cell_count", "null_cell_percentage")
    return result
  }

  /**
    * This function counts the not a number cell number
    *
    * @author Pengfei liu
    * @version 1.0
    * @since 2020-01-27
    * @param df      source data frame
    * @param colName second column value to be merged
    * @return Long, It returns the number of null cell
    **/

  def getNaNCount(df: DataFrame, colName: String): Long = {
    df.select(colName).filter(col(colName).isNaN).count()
  }

  /**
    * This function uses regular expression to check if a string value is a digit or not.
    *
    * @author Pengfei liu
    * @version 1.0
    * @since 2020-01-27
    * @param value A string value
    * @return Boolean
    **/
  def isDigit(value: String): Boolean = {
    if (value ==null) return true
    else if (value.equals(" ")) return false
    else {
      // we use regular expression,
      // ^-? : It means it may starts with -
      // [0-9]+ : followed by at least one digit between 0 and 9
      // (\.|,) : It can be separated by . or , we need protect . with \. because its a key word in regular expression.
      // [0-9]+ : followed by at least one digit.
      // ((\.|,)[0-9]+)? : means this is optional.
      return value.matches("^-?[0-9]+((\\.|,)[0-9]+)?$")
    }
  }

  /**
    * This function returns a new data frame which contains a new column which indicates the target column contains
    * digits or not
    *
    * @author Pengfei liu
    * @version 1.0
    * @since 2020-01-27
    * @param df      source data frame
    * @param colName the column name which we need to check if it contains no digit number or not
    * @return DataFrame
    **/

  def getIsDigitDF(df: DataFrame, colName: String): DataFrame = {
    val spark = df.sparkSession
    import spark.implicits._
    //register a udf for isDigit method
    spark.udf.register("isDigit", (arg: String) => isDigit(arg))

    //create column colName_isDigit,
    df.withColumn(s"${colName}_isDigit", expr(s"isDigit($colName)"))

  }

  /**
    * This function is the overload version of getIsDigitDF, it takes a list of column names, and returns a new data
    * frame which contains a new column for each target column which indicates the target column contains
    * digits or not
    *
    * @author Pengfei liu
    * @version 1.0
    * @since 2020-01-27
    * @param df       source data frame
    * @param colNames A list of the column name which we need to check if it contains no digit number or not
    * @return DataFrame
    **/

  def getIsDigitDF(df: DataFrame, colNames: List[String]): DataFrame = {
    var result = df
    for (colName <- colNames) {
      result = getIsDigitDF(result, colName)
    }
    return result
  }

  /**
    * This function takes a data frame produced by getIsDigitDF, it counts the cell that is not a digit
    * and calculates a percentage based on the total number, then returns these information as a data frame
    *
    * @author Pengfei liu
    * @version 1.0
    * @since 2020-01-27
    * @param df source data frame
    * @return DataFrame
    **/
  def getIsNotDigitCount(df: DataFrame): DataFrame = {
    val spark = df.sparkSession
    import spark.implicits._
    // get column names as an array
    val colNames = df.columns.toArray
    val totalCount = df.count()
    // create a buffer to store result before converting to data frame
    val buf = scala.collection.mutable.ListBuffer.empty[(String, Long, Double)]
    for (colName <- colNames) {
      val index = colName.lastIndexOf("_isDigit")
      if (index > 1) {
        val sourceColName = colName.substring(0, index)
        val noDigitCount = df.filter(col(colName) === false).count()
        val percentage: Double = (noDigitCount.toDouble / totalCount.toDouble) * 100
        buf.append((sourceColName, noDigitCount, percentage))
      }
    }
    buf.toList.toDF("column_name", "isNotDigit_cell_count", "isNotDigit_cell_percentage")
  }

  /**
    * This function takes a data frame produced by getIsDigitDF, it shows distinct values of the cell that is not a digit
    *
    * @author Pengfei liu
    * @version 1.0
    * @since 2020-01-27
    * @param df source data frame
    * @param colNames list of column names that contains cell which is not digit
    * @return DataFrame
    **/
  def showNotDigitValues(df:DataFrame,colNames:List[String])={
    for(colName<-colNames){
     val badValues= df.filter(col(s"${colName}_isDigit")===false).select(colName).distinct()
     badValues.show(badValues.count().toInt,false)
    }
  }

  /** **************************************************************************************************************
    * ************************************** error1.csv ***********************************************************
    * ************************************************************************************************************/
  /*
  Beijin,2016,100.0
Warsaw,2017,200
Boston,2015,ok
,,
Benxi,love,150
Toronto,2017,50
GuangZhou,2017,50
,,
,,
,,


  * */

}
