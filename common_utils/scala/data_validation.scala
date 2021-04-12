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
    * @param value     A string value
    * @return Boolean
    **/
  def isDigit(value: String): Boolean = {
      if (value ==null) return true
    if (value.equals(" ")) return false
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
    val spark=df.sparkSession
    import spark.implicits._
    //register a udf for isDigit method
    spark.udf.register("isDigit",(arg:String)=>isDigit(arg))

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
    * @param df      source data frame
    * @param colNames A list of the column name which we need to check if it contains no digit number or not
    * @return DataFrame
    **/

  def getIsDigitDF(df: DataFrame, colNames: List[String]): DataFrame = {
    var result=df
    for(colName<-colNames){
     result=getIsDigitDF(result,colName)
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
  
  
   /**
    * This function takes a data frame produced by getIsDigitDF, it returns a new data frame which contains only the
    * lines with bad values(String value in a digit column). 
    *
    * @author Pengfei liu
    * @version 1.0
    * @since 2020-01-27
    * @param df source data frame
    * @return DataFrame
    **/
  def showLinesWithBadValue(df:DataFrame):DataFrame={
    val spark=df.sparkSession
     // get column names as an Array
    val colNames=df.columns.toArray
    // get schema of the data frame
    val schema=df.schema
    // to create an empty data frame with a specific schema, we need to use the sparkContext to create an empty RDD
    val sc=spark.sparkContext
    var result:DataFrame=spark.createDataFrame(sc.emptyRDD[Row], schema)
    for(colName<-colNames){
      if(colName.contains("_isDigit")){
        result=result.union(df.filter(col(colName)===false))
      }
    }
    return result
  }
