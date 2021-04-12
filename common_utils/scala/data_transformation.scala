/**
    * This method transform multi rows of an object into columns, after the transformation, for each object we only have
    * one row in the data frame. To make the transformation, this method cast all column to type string, as we don't do
    * arthimetic operations here. So it won't be a problem. You can change the type back to Int, or double after the
    * transformation.
    * @author Pengfei liu
    * @version 1.0
    * @since 2018-12-20
    * @param rawDf The source data frame in which the transformation will take place.
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
    * This function takes a data frame, a list of column names, a old value, and a new value, it will replace the old
    * value by the new value in all given columns of the data frame.
    *
    * @author Pengfei liu
    * @version 1.0
    * @since 2018-12-20
    * @param rawDf The source data frame.
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
     /* To understand the following function, it's better to break it into two parts
     * 1. val x:Seq[(Int,String)] = colNameNumMap.toSeq.sortWith(_._1 < _._1)
     *
     * 2. ListMap(x:_*)
     *
     * The sortWith function returns a sequence of tuples, it takes a boolean expression, in
     * our example, _._1 means the first element of a tuple. We can also replace the sortWith
     * function with sortBy(_._1), which means sort the sequence by using the first element of the
     * tuples, from low to high. It also returns a sequence of tuples.
     *
     * The _* is used to convert the data so it will be passed as multiple parameters. In our example,
     * x has a Sequence type, but x:_* has tuple type
     * */

    ListMap(colNameNumMap.toSeq.sortWith(_._1 < _._1):_*)
  }
