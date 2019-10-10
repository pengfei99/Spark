package org.pengfei.Lesson04_Spark_SQL

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.pengfei.Lesson04_Spark_SQL.Lesson04_5_Spark_DataSet.{EmailArrayBody, EmailStringBody, SalesByCity}

object Lesson04_6_Spark_SQL_Built_In_Functions {

  def main(args:Array[String]):Unit={
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val spark = SparkSession.builder().master("local[2]").appName("Lesson4_1_Saprk_SQL_Intro").getOrCreate()

    /**********************************************************************************************************
      * *********************************************4.5.6 DataSet built-in functions**************************
      * ************************************************************************************************/
    /* Spark SQL comes with a comprehensive list of built-in functions, which are optimized for fast execution. It
       * implements these functions with code generation techniques. The built-in functions can be used from both
       * the DataFrame API and SQL interface.
       *
       * To use Spark’s built-in functions from the DataFrame API, you need to add the following import
       * statement to your source code.
       * import org.apache.spark.sql.functions._
       *
       * The built-in functions can be classified into the following categories:
       * - aggregate
       * - collection
       * - date/time
       * - math
       * - string
       * - window
       * - miscellaneous functions.
       * */

    /**************************************4.5.6.1 Aggregate functions **********************************/
   // DSAggOperations(spark)

    /**************************************4.5.6.2 Collection functions **********************************/

   // DSCollectionOperations(spark)

    /**************************************4.5.6.3 Date/time functions **********************************/

    // DSDateTimeOperations(spark)

    /**************************************4.5.6.4 Math functions **********************************/

     // DSMathOperations(spark)
    /**************************************4.5.6.5 String functions **********************************/

      DSStringOperations(spark)
    /**************************************4.5.6.6 Window functions **********************************/

      // DSWindowOperations(spark)

    /**************************************4.5.6.7 Miscellaneous functions **********************************/

     // DSMiscellaneousOperations(spark)

    /*************************************4.5.6.8 UDFs and UDAFs ***********************************************/
   // DSUDFOperations(spark)
  }

  /*******************************************************************************************************
    **********************************************4.5.6.1 Aggregate functions ****************************
    * ***************************************************************************************************/

  def DSAggOperations(spark: SparkSession):Unit={

    /* The aggregate functions can be used to perform aggregations on a column. The built-in aggregate
     * functions
     * - approxCountDistinct
     * - avg
     * - count
     * - countDistinct
     * - first
     * - last
     * - max
     * - mean
     * - min
     * - sum
     * - sumDistinct.*/
    import spark.implicits._
    val salesByCity = List(SalesByCity(2014, "Boston", "MA", "USA", 2000),
      SalesByCity(2015, "Boston", "MA", "USA", 3000),
      SalesByCity(2014, "Cambridge", "MA", "USA", 2000),
      SalesByCity(2015, "Cambridge", "MA", "USA", 3000),
      SalesByCity(2014, "Palo Alto", "CA", "USA", 4000),
      SalesByCity(2015, "Palo Alto", "CA", "USA", 6000),
      SalesByCity(2014, "Pune", "MH", "India", 1000),
      SalesByCity(2015, "Pune", "MH", "India", 1000),
      SalesByCity(2015, "Mumbai", "MH", "India", 1000),
      SalesByCity(2014, "Mumbai", "MH", "India", 2000)).toDF()

    /* After a group by you can do simple aggregate functions, if you want to do agg on multiple columns, you need to use
    * agg method*/
    val groupByCityAndYear=salesByCity.groupBy("city").sum("revenue") as "amount"
    val groupByYear=salesByCity.groupBy("year").avg("revenue")

    groupByCityAndYear.show()
    groupByYear.show()
    /*********************************agg*******************************************/
    /* The agg method performs specified aggregations on one or more columns in the source DataFrame and
     * returns the result as a new DataFrame. You need to import org.apache.spark.sql.functions._*/
    val aggregates = salesByCity.groupBy("year").agg(min("revenue"),max("revenue"),sum("revenue"),count("year"))
    aggregates.show()



    /**********************************************************************************************************
      * ******************************************** 4.5.6.2 Collection ***************************************
      *********************************************************************************************************/
  }
  def DSCollectionOperations(spark:SparkSession):Unit={
    import spark.implicits._

    /* The collection functions operate on columns containing a collection of elements. The built-in collection functions
    * includes :
    * - array_contains :
    * - explode :
    * - size :
    * - sort_array:
    * */
    val emailsWithArrayBody=List(EmailArrayBody("James", "Mary", "back", Array("just", "got", "vacation")),
      EmailArrayBody("John", "Jessica", "money", Array("make", "dollars")),
      EmailArrayBody("Tim", "Kevin", "report", Array("send", "ASAP"))).toDF()
    val emailsWithStringBody = List(EmailStringBody("James", "Mary", "back", "just got back from vacation"),
      EmailStringBody("John", "Jessica", "money", "make million dollars"),
      EmailStringBody("Tim", "Kevin", "report", "send me sales report ASAP")).toDF()

    /***************************************Array_contains**************************************************/
    /* The array_contains method check if an (array) Columns contains a certain element, it returns a boolean*/
    println("*********************************************Array_contains**************************************")

    val haveSend=emailsWithArrayBody.withColumn("haveSend",array_contains($"body","send"))
    haveSend.show()

    /***************************************size**************************************************/
    /* The size method counts the size of array in the specific column. it returns an int*/
    println("********************************************* size **************************************")

    val wordsCount=emailsWithArrayBody.withColumn("wordsNB",size($"body"))
    wordsCount.show()

    /***************************************sort_array**************************************************/
    /* The sort_array method sort the array of the specific columns, it returns a new sorted array*/
    println("*********************************************sort_array**************************************")

    val sortBody=emailsWithArrayBody.withColumn("sortedBody",sort_array($"body"))
    sortBody.show()

    /***************************************Explode****************************************************/
    /* The explode method generates zero or more row from a column using a user provided function. It takes three
    * arguments. The first argument is the input column, the second argument is the output column and the third
    * argument is a user provided function that generates one or more value for the output column for each value
    * in the input column.
    *
    * This method(dataset.explode) is deprecated since spark 2.0. Use flatMap or select with functions.explode()
    *
    * */
    println("*********************************************Explode**************************************")

    // you must import org.apache.spark.sql.functions.explode for this to work
    // The explode works well with column of array or map type, With string type, it does not work.
    val wordDf=emailsWithArrayBody.withColumn("words",explode($"body"))
    wordDf.show()

    // The simple way is to transform string type to Array type
    def splitStringToArray(input: String):Array[String] = {
      input.split(" ")
    }
    spark.udf.register("splitStringToArray",(arg:String)=>splitStringToArray(arg))
    val emailStringBodyTrans=emailsWithStringBody.withColumn("ArrayBody", expr("splitStringToArray(body)"))
    emailStringBodyTrans.show()
    val wordDfFromString=emailStringBodyTrans.withColumn("words",explode($"ArrayBody"))
    wordDfFromString.show()


    /* use explode with udf, failed need to revisit
    val flatMapUdf=spark.udf.register("myFlatMapFunction",myFlatMapFunction(String))
    val wordDf1=emailsWithStringBody.withColumn("words",explode(myFlatMapFunction($"body")))
    wordDf1.show()
    */
    val wordDfDep=emailsWithStringBody.explode("body","words"){body:String=>body.split(" ")}
    wordDfDep.show()
  }


  /**********************************************************************************************************
    * ******************************************** 4.5.6.3 Date/Time ***************************************
    *********************************************************************************************************/
  def DSDateTimeOperations(spark:SparkSession):Unit={
    import spark.implicits._

    /* The date/time functions make it easy to process columns containing date/time values. These functions
     * can be further sub-classified into the following categories:
     * - conversion
     * - extraction
     * - arithmetic
     * - miscellaneous
     * */

    /***********************************************Conversion***************************************************/

    /* The conversion functions convert data/time values from one format to another. For example, you can convert a time
    * string yyyy-MM-dd HH:mm:ss to a Unix epoch value using the unix_timestamp function. The from_unixtime function
    * converts a Unix epoch value to a string representation. The built-in conversion function include:
    * - unix_timestamp
    * - from_unixtime
    * - to_date
    * - quarter
    * - day
    * - dayofyear
    * - weekofyear
    * - from_utc_timestamp
    * - to_utc_timestamp
    * */

    /* Scala doesn't have its own lib for Dates and timestamps, so we need to use java lib for current time*/


    val dateDF=List("2017-09-16","2017-09-17","2017-09-18","2017-12-31","2017-03-20").toDF("date")
    dateDF.show()
    /**************************************unix_timestamp*****************************************/
    /* unix_timestamp take a column, and second argument date format is optional. if no format is provided, the
     * default format will be yyyy-MM-dd HH:mm:ss. It returns null if conversion fails. The unix_timestamp is a
     * bigint which describes number of seconds that have elapsed since 0 (UTC) 01/01/1970.
     * */

    val unix_timeDF=dateDF.withColumn("unix_timestamp",unix_timestamp($"date","yyyy-MM-dd"))
    unix_timeDF.show()

    /***************************************from_unixtime****************************************/
    /* from_unixtime takes a column, second argument date fromat is optional. if no format is provided, the
     * default format will be yyyy-MM-dd HH:mm:ss. It returns null if conversion fails.*/
    val backToDateStringDF=unix_timeDF.withColumn(("date_string"),from_unixtime($"unix_timestamp"))
    backToDateStringDF.show()

    /******************************************to_date*********************************************/
    /* to_date method takes a column of string date, then convert it to a date object. */
    val dateObjDf= dateDF.withColumn("dateObj",to_date($"date","yyyy-MM-dd"))
    dateObjDf.show()

    /****************************************quarter/dayofmonth/dayofyear/weekofyear***********************/

    /* quarter method takes a column of date(string,timestamp,dateObj), then returns the quarter of the year
    * in the range 1 to 4.
    * Example: quarter('2015-04-08') = 2.*/

    val quarterDf=dateDF.withColumn("quarter",quarter($"date"))
    quarterDf.show()

    /* dayofyear method takes a column of date(string,timestamp,dateObj), then returns the day of the year in the
    * range 1 to 365.  Example dayofyear('2017-09-16') = 259 */
    val dayDf=dateDF.withColumn("dayOfYear",dayofyear($"date"))
    dayDf.show()

    /* dayofmonth method takes a column of date(string,timestamp,dateObj), then returns the day of the month in the
    * range 1 to 31.  Example dayofmonth('2017-09-16') = 16 */
    val dayMonDf=dateDF.withColumn("dayOfMonth",dayofmonth($"date"))
    dayMonDf.show()

    /* weekofyear method takes a column of date(string,timestamp,dateObj), then returns the week of the year in the
    * range 1 to 52.  Example weekofyear('2017-09-16') = 37 */
    val weekDf=dateDF.withColumn("weekOfYear",weekofyear($"date"))
    weekDf.show()

    /* current_date function returns the current date in date object type. The date_format mehtod
    * can convert the date object to string with the given format*/
    val currentDateDf=dateDF.withColumn("currentDate",date_format(current_date(), "y-MM-dd'T'hh:mm:ss.SSS'Z'"))
     currentDateDf.show()

    /* current_timestamp function returns the current date in type timestamp. */
    val currentTimestampDf=dateDF.withColumn("currentTimestamp",current_timestamp())
    currentTimestampDf.show()
    /* from_utc_timestamp function convert a unix timestamp to a specific timezone date. In our case, it's "CET", It
    * can be "EST" or any timezone code*/
    val cetZoneDf=currentTimestampDf.withColumn("CET_Date",from_utc_timestamp($"currentTimestamp","CET"))
    cetZoneDf.show()

    /************************************Date arithmetic functions****************************************/
    /* The arithmetic functions allow you to perform arithmetic operation on columns containing dates. For
    * example, you can calculate the difference between two dates, add days to a date, or subtract days from a
    * date. The built-in date arithmetic functions include:
    * - datediff(end:Column,start:Column) -> Returns the number of days from start to end.
    * - date_add(start:Column, int days) -> Returns the date that is days days after start
    * - date_sub(start:Column, int days) -> Returns the date that is days days before start
    * - add_months(StartDate:Column, numMonths:Int) -> Returns the date that is numMonths after startDate.
    * - last_day(date:Column) -> Given a date column, returns the last day of the month which the given date belongs to.
    * - next_day(date:Column,dayOfWeek:String) -> Given a date column, returns the first date which is later than the
    *                                            value of the date column that is on the specified day of the week.
    *                                            For example, next_day('2015-07-27', "Sunday") returns 2015-08-02
    *                                            because that is the first Sunday after 2015-07-27.
    *                                            Day of the week parameter is case insensitive, and accepts: "Mon",
    *                                            "Tue", "Wed", "Thu", "Fri", "Sat", "Sun".
    * - months_between(date1:Column,date2:Column) -> Returns number of months between dates date1 and date2.
    *
    * */

    /* datediff can only give the difference of two date in days, if you want to get second, hour difference, you need
    * to convert the date in unix time and do end-start. for datediff, the two columns can be type
    * of string, dateObject,timestamp, unix time. you don't need to convert them to the same type or format*/
    val currentDateObj=currentDateDf.withColumn("dateObj",to_date($"date"))
    //currentDateObj.show()
    val dateDiff=currentDateObj.withColumn("dayDiff",datediff($"currentDate",$"date"))
    dateDiff.show()
    /* date_add , date_sub example*/
    currentDateObj.select(date_add($"currentDate",180)).show()
    currentDateDf.select(date_sub($"currentDate",180)).show()
    /* next_day*/
    currentDateDf.select(next_day($"currentDate","Fri")).show()
    /* months_between*/
    currentDateDf.select(months_between($"currentDate",$"date")).show()



  }
  /**********************************************************************************************************
    * ******************************************** 4.5.6.4.1 Math functions ***************************************
    *********************************************************************************************************/
  def DSMathOperations(spark:SparkSession):Unit={
    import spark.implicits._
/* The math functions operate on columns containing numerical values. Spark SQL comes with a long list of
 * built-in math functions. Example include : The return value of the below function are all Columns.
 * - abs(arg:Column) : Column -> compute the absolute value of the column
 * - acos (arg:Column/cName:String) : Column -> compute the cosine inverse of the given range 0.0 through pi
 * - approx_count_distinct(e:Column/cName:String,double rsd) -> Aggregate function: returns the approximate number
 *                                                             of distinct items in a group.
 * - ceil(e: Column) : Computes the ceiling of the given value.
 * - cos(e: Column) : computes the cosine of the given value.
 * - exp(e: Column) : Computes the exponential of the given value.
 * - factorial ""  : Computes the factorial of the given value.
 * - floor : Computes the floor of the given value
 * - hex : Computes hex value of the given column. (base 16)
 * - hypot(a:Column,b:Column): Computes sqrt(a^2 + b^2) without intermediate overflow or underflow hypot(3,4)=5
 * - log(e:column) : Computes the natural logarithm of the given value.
 * - log(base:Double, e:Column): Returns the first argument-base logarithm of the second argument.
 * - log10 : Computes the logarithm of the given value in base 10.
 * - pow(e1:Column,e2:Column) : Returns the value of the first argument raised to the power of the second argument. e2
 *                              can be a double
 * - round : Returns the value of the column e rounded to 0 decimal places with HALF_UP round mode.
 * - shiftLeft : Shift the given value numBits left
 * - sqrt : Computes the square root of the specified float value.
 * and other commonly used math functions
 *
 * Floor and ceiling give us the nearest interger of a number. For example, floor(5.2), ceil(5.2)=6,
 * floor(5)=5, ceil(5)=5*/
val nums=List(NumTest(234.5,1,30),
  NumTest(23.45,2,60),
  NumTest(2.345,3,90),
  NumTest(0.2345,4,180)).toDF()

    nums.show()
/* round is a function that rounds a numeric value to the specified precision. When the given precision is a
 * positive number, a given input numeric value is rouded to the decimal position specified by the the
 * precision. When the specified precision is a zero or a negative number, a given input numeric value is
 * rounded to the position of the integral part specified by the precision.
 *
 * For example round(23.45,0)=23,round(23.45,1)=23.5,round(23.45,-1)=20 */
    nums.select(round($"doubleNum",0).alias("round0"),
               round($"doubleNum",1).alias("round1"),
               round($"doubleNum",-1).alias("round-1")).show()

    /* sin,tan,cos take a column of int (angle), and returns the sin,cos,tan of the int*/

    nums.select(sin($"angleNum").alias("sin"),
      cos($"angleNum").alias("cos"),
      tan($"angleNum").alias("tan")).show()

    /* Pow method takes two argument, 1st is digital column, 2nd is the puissance number. it returns the puissance of
    * the column*/
    nums.select($"logNum",pow($"logNum",2)).show()

    val genNumDf= nums.select($"logNum".alias("id"),
      rand(seed=10).alias("uniform1"),
      rand(seed=88).alias("uniform2"),
      randn(seed=27).alias("normal"))

    // we can combine as many function as we want in a line. the following example we calculate the
    // the square root of the sin(a)^2+cos(a)^2
    val degreeDf=genNumDf.select($"uniform1",sqrt(pow(sin($"uniform1"),2)+pow(cos($"uniform1"),2)))
    degreeDf.show()
  }

  /**********************************************************************************************************
    * ******************************************** 4.5.6.4.2 Statistic functions ******************************
    *********************************************************************************************************/
  def DSStatOperations(spark:SparkSession):Unit={
    import spark.implicits._

    val nums=List(NumTest(234.5,1,30),
      NumTest(23.45,2,60),
      NumTest(2.345,3,90),
      NumTest(0.2345,4,180)).toDF()
    /********************************************Generate random numbers**************************************/
    /* rand is for generating values from a distribution, rand is the uniform distribution,
   * randn is the standard normal distribution*/

    val genNumDf= nums.select($"logNum".alias("id"),
      rand(seed=10).alias("uniform1"),
      rand(seed=88).alias("uniform2"),
      randn(seed=27).alias("normal"))
    genNumDf.show()

    /**************************************Basic stat functions**********************************/
    /*mean,min,max,avg takes a numeric column and return the mean, min, max, avg of the column*/
    genNumDf.select(mean($"uniform1"),min($"normal"),avg($"normal"),max($"uniform1")).show()

    /***********************************Covariance ***********************************************/
    /* Covariance is a measure of how two variables change with respect to each other. A positive number would mean
    *that there is a tendency that as one variable increases, the other increases as well. A negative number would mean
    * that as one variable increases, the other variable has a tendency to decrease. The sample covariance of two columns
    * of a DataFrame can be calculated as follows:
    *
    * Since spark2.3 cov is replaced by covar_pop and covar_samp
    * covar_pop -> Aggregate function: returns the population covariance for two columns.
    * covar_samp -> Aggregate function: returns the sample covariance for two columns.*/

    val covOfUni=genNumDf.stat.cov("uniform1","uniform2")
    println(s"covariance between uniform1 and uniform2: ${covOfUni}")
    val covOfId=genNumDf.stat.cov("id","id")
    println(s"covariance between id and id: ${covOfId}")

    /* As you can see from the above, the covariance of the two randomly generated columns is close to zero,
    * while the covariance of the id column with itself is very high.
    *
    * The covariance value of 9.17 might be hard to interpret. Correlation is a normalized measure of covariance
    * that is easier to understand, as it provides quantitative measurements of the statistical dependence */

    /* corr is a Aggregate function: returns the Pearson Correlation Coefficient for two columns.*/
    val corrOfUni=genNumDf.stat.corr("uniform1","uniform2")
    println(s"correlation between uniform1 and uniform2: ${corrOfUni}")
    val corrOfId=genNumDf.stat.corr("id","id")
    println(s"correlation between id and id: ${corrOfId}")

    /******************************************************Cross Tabulation****************************************/

    /* Cross Tabulation provides a table of the frequency distribution for a set of variables. Cross-tabulation is
    * a powerful tool in statistics that is used to observe the statistical significance (or independence) of variables.
    * Since Spark 1.4, users will be able to cross-tabulate two columns of a DataFrame in order to obtain the counts of
    * the different pairs that are observed in those columns. Here is an example on how to use crosstab to
    * obtain the contingency table.*/
    val names = List("Alice", "Bob", "Mike")
    val items = List("milk", "bread", "butter", "apples", "oranges")

    val genList=generateDf(names,items,100)
    // println(s"genList ${genList.mkString(",")}")
    val genDf=genList.toDF()
    genDf.stat.crosstab("name","item").show()
    /* One important thing to keep in mind is that the cardinality of columns we run crosstab on cannot be too big.
    * That is to say, the number of distinct “name” and “item” cannot be too large. Just imagine if “item” contains
    * 1 billion distinct entries: how would you fit that table on your screen?!*/

    /**********************************************Frequent Items**************************************/
    /* Figuring out which items are frequent in each column can be very useful to understand a dataset.
    *  In the stat lib, we have freqItems method takes two arguments, 1st is a collection of column
    *  name(List, seq, array are ok), 2nd(optional) is the minimum percentage of the frequency.  */

    val freqItem=genDf.stat.freqItems(List("name","item"),0.4)
    val freqName=genDf.stat.freqItems(Seq("name"))
    freqItem.show()
    freqName.show()

    /* We can combine two columns to one columns as an Array.Then check the frequency of the combined column*/
    val genCombineListDf=genDf.withColumn("name_item",struct("name","item"))
    genCombineListDf.show(5)

    val freqCombine=genCombineListDf.stat.freqItems(Seq("name_item"),0.4)
    freqCombine.show()
  }
  /**********************************************************************************************************
    * ******************************************** 4.5.6.5 String functions ***************************************
    *********************************************************************************************************/
  def DSStringOperations(spark:SparkSession):Unit={
    import spark.implicits._
/* Spark SQL provides a variety of built-in functions for processing columns that contain string values. For
 * example, you can split, trim or change case of a string. The built-in string functions include:
 * - ascii(e:Column): Computes the numeric value of the first character of the string column, and returns the result
 *                    as an int column.
 * - base64(e:Column): Computes the BASE64 encoding of a binary column and returns it as a string column.
 * - unbase64(e:Column): Decodes a BASE64 encoded string column and returns it as a binary column.
 * - concat(e*:Column): Concatenates multiple input columns together into a single column.
 * - concat_ws(sep:String,e*:Column): Concatenates multiple input string columns together into a single string column,
 *                                    using the given separator. The columns could be in a form of a collection of cols.
 * - decode(e:Column,charset:String): Computes the column into a string from a binary using the provided
 *                                    character set (one of 'US-ASCII', 'ISO-8859-1', 'UTF-8', 'UTF-16BE', 'UTF-16LE',
 *                                    'UTF-16').
 * - encode(e:Column,charset:String): Computes the column into a binary from a string using the provided character
 *                                    set (one of 'US-ASCII', 'ISO-8859-1', 'UTF-8', 'UTF-16BE', 'UTF-16LE', 'UTF-16').
 * - format_number(e:Column,int d): Formats numeric column x to a fromat like '###,###,###.##'for example a number
 *                                  123456.78 will be transform into 123,456.78. rounded to d decimal places with
 *                                  HALF_EVEN round mode, and returns the result as a string column.
 *                                  If d is 0, the result has no decimal point or fractional part. If d is
 *                                  less than 0, the result will be null.
 * - format_string(format:String,e*:Column): Formats the argument columns in printf-style and returns the
 *                                           result as a string column.
 * - get_json_object(e:Column,path:String): Extracts json object from a json string based on json path specified,
 *                                         and returns json string of the extracted json object.
 * - initcap(e:Column): Returns a new string column by converting the first letter of each word to uppercase.
 * - instr(e:Column,subStr:String): Locate the position of the first occurrence of substr column in the given string.
 *                                  Returns null if either of the arguments are null.
 * - length(e:Column): Computes the character length of a given string or number of bytes of a binary string.
 *                     The length of character strings include the trailing spaces. The length of binary strings
 *                     includes binary zeros.
 * - levenshtein(l:Column,r:Column): Computes the Levenshtein distance of the two given string columns.  Informally,
 *                                   the Levenshtein distance between two words is the minimum number of
 *                                   single-character edits (insertions, deletions or substitutions) required
 *                                   to change one word into the other
 * - locate(subStr:String,str:Column): Locate the position of the first occurrence of substr of a string column.
 * - lower(e:Column): Converts a string column to lower case.
 * - upper(e:Column): Converts a string column to upper case.
 * - lpad(e:Column,len:Int,pad:String): Left-pad the string column with pad to a length of len.
 * - rpad(e:Column,len:Int,pad:String): Right-pad the string column with pad to a length of len.
 * - ltrim(e:Column): Trim the spaces from left end for the specified string value. "  a   " => "a   "
 * - rtrim(e:Column,trimStr:String): Trim the specified character string from right end for the specified string column.
 *                                   if the trimStr arg is empty, it will trim the space. "  a   " => "  a",
 * - trim(e:Column,trimStr:String): Trim the specified character from both ends for the specified string column.
 *                                   if the trimStr arg is empty, it will trim the space. "  a   " => "a"
 * - singleSpace(e:Column): Can removes all extra space between words. This come from a 3rd party lib spark-daria
 *                          https://github.com/MrPowers/spark-daria/
 * - regexp_extract(e:Column,exp:String,groupIdx:Int): Extract a specific group matched by a Java regular expr, from
 *                                                    the specified string column.
 * - regexp_replace(e:Column,pattern:Column,replacement:Column): Replace all substrings of the specified string value
 *                                           that match regexp with replacement
 * - repeat(e:Column,n:Int): Repeats a string column n times and returns it as a new string column.
 * - reverse(e:Column): Reverses the string column and returns it as a new string column.

 * - soundex(e:Column): Returns the soundex code for the specified expression. Soundex is a phonetic algorithm for
 *                     indexing words(string) by sound in English. It can match words despite minor differences in
 *                     spelling. For example  both "Robert" and "Rupert" return the same string "R163", while "Rubin"
 *                     yields "R150". So Robert and Rupert is more similar than Rubin.
 * - split(e:Column,pattern:String): Splits string column around pattern (pattern is a regular expression)
 * - substring(e:Column,position:Int,len:Int): Substring starts at pos and is of length len when str is String type
 *                                             or returns the slice of byte array that starts at pos in byte and is
 *                                             of length len when column is Binary type
 * - substring_index(e:Column,delim:String,count:Int): Returns the substring from string str before count occurrences
 *                                                    of the delimiter delim.
 * - translate(src:Column,matchStr:String,replaceStr:String): Translate any character in the src by a character in
 *                                                            replaceString.
 * and other commonly used string functions.*/

    val nasdaqDf=List(Nasdaq(1,"1347 Captial Corp.","TFSC","9.43","$56.09M",56090000,"2014","Finance","Business Service",
      "http://www.nasdaq.com/symbol/tfsc"),
      Nasdaq(2,"1347 Property Insurance.","PIH","7.66","$48.7M",48700000,"n/a","Finance","Property-Casualty Insurance",
        "http://www.nasdaq.com/symbol/pih"),
      Nasdaq(300,"1-800 FLOWERS.COM","FLWS","10.32","$667.78M",667780000,"1999","Consumer Services","Other Speciality",
        "http://www.nasdaq.com/symbol/flws")
    ).toDF()
    nasdaqDf.show()

    /*For example, we want to fix the length of ClientId(e.g. 1->00000001), we can use functions such as lpad, rpad
     * The number of 0 lpad add is auto determine to fit the length of 8 */
    nasdaqDf.select($"clientID",lpad($"clientID",8,"0").alias("new_clientID")).show()
    nasdaqDf.select($"symbol",rpad($"symbol",6,"0").alias("new_symbol")).show()

    /* To make big Int to String with delimiter, we can use format_number*/
    nasdaqDf.select($"marketCapAmount",format_number($"marketCapAmount",0)).show()

    /* format_string method can helps to build new string based on existing columns. The following example, we add Year:
    * in front of ipoYear*/
    nasdaqDf.select($"ipoYear",format_string("Year: %s",$"ipoYear")).show()

    /* reverse every letter of the string*/
    nasdaqDf.select($"name",reverse($"name")).show()
    /* trim only remove all leading and trailing whitespace, space between words are safe*/
    nasdaqDf.select($"name",trim($"name")).show()


    /* split example*/
    nasdaqDf.select($"industry",split($"industry"," ")).show()

    /* regexp_extract third argument groupIdx is the group id of the regluar expression. For example if we have a
    * regular expression (A)(B)(C)(D). A has groupIdx 1, B=2,C=3,D=4. Check this site https://regexr.com/ it can explain
    * how to use regular expression*/
    nasdaqDf.select($"summaryQuote",regexp_extract($"summaryQuote","(http://)(.*?com)(/symbol/)([a-z]*)",2)).show(false)

    // We can also replace sub string in a column which matches a regular expression. 2nd and 3rd args can be column or
    //string. The following example replace http by https
    nasdaqDf.select(regexp_replace($"summaryQuote","http://","https://")).show()
    //Here we replace url.com by pengfei.org
    nasdaqDf.select(regexp_replace($"summaryQuote","[a-z]*.com","pengfei.org")).show(false)
    //We can also use translate to replace string column.
    nasdaqDf.select($"ipoYear",translate($"ipoYear","n/a","8888")).show()

    /*get levenshtein value between industry and sector*/
    nasdaqDf.select(levenshtein($"industry",$"sector")).show()

    /******************************************SubString************************************************/
    val subNasdaqDf=nasdaqDf.withColumn("Sub_ipoYear", substring($"ipoYear",0,2)).show()


  }

  /**********************************************************************************************************
    * ******************************************** 4.5.6.6 Window functions ***************************************
    *********************************************************************************************************/
  def DSWindowOperations(spark:SparkSession):Unit={

    import spark.implicits._

    /* empDF is the normal data , empDF1 has the same deptno for all users*/
    val empDF = spark.createDataFrame(Seq(
      (7369, "SMITH", "CLERK", 7902, "17-Dec-80", 800, 20, 10),
      (7499, "ALLEN", "SALESMAN", 7698, "20-Feb-81", 1600, 300, 30),
      (7521, "WARD", "SALESMAN", 7698, "22-Feb-81", 1250, 500, 30),
      (7566, "JONES", "MANAGER", 7839, "2-Apr-81", 2975, 0, 20),
      (7654, "MARTIN", "SALESMAN", 7698, "28-Sep-81", 1250, 1400, 30),
      (7698, "BLAKE", "MANAGER", 7839, "1-May-81", 2850, 0, 30),
      (7782, "CLARK", "MANAGER", 7839, "9-Jun-81", 2450, 0, 10),
      (7788, "SCOTT", "ANALYST", 7566, "19-Apr-87", 3000, 0, 20),
      (7839, "KING", "PRESIDENT", 0, "17-Nov-81", 5000, 0, 10),
      (7844, "TURNER", "SALESMAN", 7698, "8-Sep-81", 1500, 0, 30),
      (7876, "ADAMS", "CLERK", 7788, "23-May-87", 1100, 0, 20)
    )).toDF("empno", "ename", "job", "mgr", "hiredate", "sal", "comm", "deptno")

    val empDF1 = spark.createDataFrame(Seq(
      (7369, "SMITH", "CLERK", 7902, "17-Dec-80", 800, 20, 10),
      (7499, "ALLEN", "SALESMAN", 7698, "20-Feb-81", 1600, 300, 10),
      (7521, "WARD", "SALESMAN", 7698, "22-Feb-81", 1250, 500, 10),
      (7566, "JONES", "MANAGER", 7839, "2-Apr-81", 2975, 0, 10),
      (7654, "MARTIN", "SALESMAN", 7698, "28-Sep-81", 1250, 1400, 10),
      (7698, "BLAKE", "MANAGER", 7839, "1-May-81", 2850, 0, 10),
      (7782, "CLARK", "MANAGER", 7839, "9-Jun-81", 2450, 0, 10),
      (7788, "SCOTT", "ANALYST", 7566, "19-Apr-87", 3000, 0, 10),
      (7839, "KING", "PRESIDENT", 0, "17-Nov-81", 5000, 0, 10),
      (7844, "TURNER", "SALESMAN", 7698, "8-Sep-81", 1500, 0, 10),
      (7876, "ADAMS", "CLERK", 7788, "23-May-87", 1100, 0, 10)
    )).toDF("empno", "ename", "job", "mgr", "hiredate", "sal", "comm", "deptno")
    /* ******************************************** 4.5.6.6.1 Window functions Intro**********************************/
 /* Spark SQL supports window functions for analytics. A window function performs a calculation across a set
* of rows that are related to the current row. The built-in window functions provided by Spark SQL include two
* categories:
*   1. Ranking functions:
*                 - rank: returns the rank of rows within a window partition
*                 - dense_rank: returns the rank of rows within a window partition, without any gaps. For example,
*                     if you were ranking a competition using dense_rank and had three people tie for second place,
*                     you would say that all three were in second place and that the next person came in third.
*                     Rank would give me sequential numbers, making the person that came in third place (after the ties)
*                     would register as coming in fifth.
*
*                 - percent_rank: returns the relative rank (i.e. percentile) of rows within a window partition.
*                 - ntile(n:Int): returns the ntile group id (from 1 to n inclusive) in an ordered window partition. For
*                          example, if n is 4, the first quarter of the rows will get value 1, the second quarter will
*                          get 2, the thirds quarter will get 3, and the last will get 4. If the rows are less than n, it
*                          works too.
*                 - row_number: returns a sequential number starting at 1 within a window partition.
*   2. Analytic functions:
*          - cume_dist: returns the cumulative distribution of values within a window partition, i.e. the fraction
*                      of rows that are below the current row. N = total number of rows in the partition.
*                      cumeDist(x) = number of values before (and including) x / N. similar to percent_rank()
*
*          - lag(e:Column,offset:Int,defaultValue:Object): returns the value that is offset rows before the current row, and null if there
*                  is less than offset rows before row. For example, an offset of one will return the previous row at
*                  any given point in the window partition. The defaultValue is optional
*          - lead(e:Column,offset:Int): returns the value that is offset rows after the current row, and null if
*                 there is less than offset rows after the current row. For example, an offset of one will return
*                 the next row at any given point in the window partition.
*          - currentRow(): Window function: returns the special frame boundary that represents the current row in
*                          the window partition.
*   3. Aggregation functions:
*          -sum(e:Column): returns the sum of selecting column for each partitions.
*          - first(e:Column): returns the first value within each partition.
*          - last(e:Column): returns the last value within each partition.
*
*
*

*
* .*/
/* ******************************************** 4.5.6.6.2 Window specification Intro **********************************/
    /* To use window functions, we need to create a window specification. A window specification defines which rows
    * are included in the frame associated with a given input row. A window specification includes three parts:
    * 1. Partitioning specification: controls which rows will be in the same partition with the given row.
    *          Also, the user might want to make sure all rows having the same value for the category column are
    *          collected to the same machine before ordering and calculating the frame. If no partitioning specification
    *          is given, then all data must be collected to a single machine.
    *
    * 2. Ordering specification: controls the way that rows in a partition are ordered, determining the position of the
    *                           given row in its partition.
    *
    * 3. Frame specification: states which rows will be included in the frame for the current input row, based on their
    *                        relative position to the current row. For example, "the three rows preceding the current
    *                        row to current row" describes a frame including the current input row and three rows
    *                        appearing before the current row.
    *
    * In spark SQL, the partition specification are defined by keyword partitionBy, ordering specification is defined by
    * keyword orderBy. The following example show a window partitionBy deptpartment number, and ordered by salary.
    * We don't define a here. */

    /*********************************** 4.5.6.6.3 Window functions example(Ranking) **********************************/
/* All the ranking function will only calculate rows in side each partitions for all partitions, for example
 * the job has 5 different values: Analyst, salesman, clerk, mananger, president. So there will be 5 partitions,
  * When we do the window function, it will do the calculation for each partition. When we do rank on salary, there is a rank
  * for analyst, a rank for salesman, etc. I change the depno, so all are from the same department, and same partition
  * so same frame. As a result, there is only one rank for rank salary. */

   /*********************************rank/denseRank/percentRank*****************************************/
   /* To be able to do rank, we need to create a window specification, In this example, we create two window specification
   * depNoWindow is a window specification partitionBy deptatement number, jobWindow is partitionBy job types*/
   val depNoWindow=Window.partitionBy($"deptno").orderBy($"sal".desc)

    val jobWindow=Window.partitionBy($"job").orderBy($"sal".desc)
    /* After creation of window specification, we can do rank in depNoWindow*/
    val rankTest=rank().over(depNoWindow)
    val denseRankTest=dense_rank().over(depNoWindow)
    val percentRankTest=percent_rank().over(depNoWindow)
    empDF.select($"*", rankTest as "rank",denseRankTest as "denseRank",percentRankTest as "percentRank").show()

    /* As we partitioned the data with column deptno, in the empDF example, we could see, rank do ranking for each dept

    empDF result:
+-----+------+---------+----+---------+----+----+------+----+---------+-----------+
|empno| ename|      job| mgr| hiredate| sal|comm|deptno|rank|denseRank|percentRank|
+-----+------+---------+----+---------+----+----+------+----+---------+-----------+
| 7788| SCOTT|  ANALYST|7566|19-Apr-87|3000|   0|    20|   1|        1|        0.0|
| 7566| JONES|  MANAGER|7839| 2-Apr-81|2975|   0|    20|   2|        2|        0.5|
| 7876| ADAMS|    CLERK|7788|23-May-87|1100|   0|    20|   3|        3|        1.0|
| 7839|  KING|PRESIDENT|   0|17-Nov-81|5000|   0|    10|   1|        1|        0.0|
| 7782| CLARK|  MANAGER|7839| 9-Jun-81|2450|   0|    10|   2|        2|        0.5|
| 7369| SMITH|    CLERK|7902|17-Dec-80| 800|  20|    10|   3|        3|        1.0|
| 7698| BLAKE|  MANAGER|7839| 1-May-81|2850|   0|    30|   1|        1|        0.0|
| 7499| ALLEN| SALESMAN|7698|20-Feb-81|1600| 300|    30|   2|        2|       0.25|
| 7844|TURNER| SALESMAN|7698| 8-Sep-81|1500|   0|    30|   3|        3|        0.5|
| 7521|  WARD| SALESMAN|7698|22-Feb-81|1250| 500|    30|   4|        4|       0.75|
| 7654|MARTIN| SALESMAN|7698|28-Sep-81|1250|1400|    30|   4|        4|       0.75|
+-----+------+---------+----+---------+----+----+------+----+---------+-----------+

    empDF1 result: all users belong to one dept
+-----+------+---------+----+---------+----+----+------+----+---------+-----------+
|empno| ename|      job| mgr| hiredate| sal|comm|deptno|rank|denseRank|percentRank|
+-----+------+---------+----+---------+----+----+------+----+---------+-----------+
| 7839|  KING|PRESIDENT|   0|17-Nov-81|5000|   0|    10|   1|        1|        0.0|
| 7788| SCOTT|  ANALYST|7566|19-Apr-87|3000|   0|    10|   2|        2|        0.1|
| 7566| JONES|  MANAGER|7839| 2-Apr-81|2975|   0|    10|   3|        3|        0.2|
| 7698| BLAKE|  MANAGER|7839| 1-May-81|2850|   0|    10|   4|        4|        0.3|
| 7782| CLARK|  MANAGER|7839| 9-Jun-81|2450|   0|    10|   5|        5|        0.4|
| 7499| ALLEN| SALESMAN|7698|20-Feb-81|1600| 300|    10|   6|        6|        0.5|
| 7844|TURNER| SALESMAN|7698| 8-Sep-81|1500|   0|    10|   7|        7|        0.6|
| 7521|  WARD| SALESMAN|7698|22-Feb-81|1250| 500|    10|   8|        8|        0.7|
| 7654|MARTIN| SALESMAN|7698|28-Sep-81|1250|1400|    10|   8|        8|        0.7|
| 7876| ADAMS|    CLERK|7788|23-May-87|1100|   0|    10|  10|        9|        0.9|
| 7369| SMITH|    CLERK|7902|17-Dec-80| 800|  20|    10|  11|       10|        1.0|
+-----+------+---------+----+---------+----+----+------+----+---------+-----------+
*/

    /* repeat above process with Job window*/
    val rankTestJob=rank().over(jobWindow)
    val denseRankTestJob=dense_rank().over(jobWindow)
    val percentRankJob=percent_rank().over(jobWindow)
    empDF.select($"*", rankTestJob as "rank",denseRankTestJob as "denseRank",percentRankJob as "percentRank").show()

    /*************************************** ntile ***********************************/
    /* In the following ntile example, we choose to divide each partition into 6 tiles (DF1 has the same dep no,
     * so one partition, DF has multiple dep no, which means multiple partition.).
     * */
    val ntileTest=ntile(6).over(depNoWindow)
    /* rowNumber on department Number window, it will stats with 1 for each partition*/
    val row_numberTest=row_number().over(depNoWindow)
    empDF.select($"*", ntileTest as "ntile", row_numberTest as "row_number").show()


    /* The ntile/rowNumber on job window(5 partition)*/
    val ntilJob=ntile(6).over(jobWindow)
    val rowNumberJob=row_number().over(jobWindow)
    empDF1.select($"*",ntilJob as "ntileJob",rowNumberJob as "rowNumberJob").show()

    /*********************************** 4.5.6.6.3 Window functions example(Analytics) *******************************/
   /***********************************lag/lead*************************************************/
   //Does not work,
    /*val lagTest=lag($"sal",1,0)
    val leadTest=lead($"sal",2,0)
    empDF1.select($"*", lagTest as "lagHireDateValue", leadTest as "leadHireDateValue").show()*/

    /*********************************cume_dist*******************************************/
    val cumeDistTest=cume_dist().over(depNoWindow)
    empDF.select($"*",cumeDistTest as "Cume Dist").show()

    //currentRow works for spark 2.3.0
    //empDF.select($"*",currentRow().over(depNoWindow) as "currentRow").show
    /************************************4.5.6.6.4 Use aggregate function in window***********************************/
    // The aggregation function will do the aggregation inside each partition. Each row stores the aggregated return
    // value
    val sumTest=sum($"sal").over(depNoWindow)
    empDF.select($"*", sumTest as "sum_of_sal").show()

    /******************************first/last ******************************************/
    val firstValue=first($"sal").over(depNoWindow)
    val lastValue=last($"sal").over(depNoWindow)

    empDF1.select($"*",firstValue as "firstValue", lastValue as "lastValue").show()
    empDF.select($"*",firstValue as "firstValue", lastValue as "lastValue").show()
/* It does not work well for the last function, the last value of the current row is always the value of the current
* row. So only the last row of the current partition returns the right value. all other rows last value is "wrong"*/

    /********************************************* 4.5.6.6.5 frame specification **********************************/
    /* There are two types of frame: ROWframe, RANGEframe. A row frame specification is defined by keyword rowsBetween().
    * A range frame specification is defined by keyword rangeBetween().
    *
    * https://databricks.com/blog/2015/07/15/introducing-window-functions-in-spark-sql.html This page explains well the
    * difference between row frame and range frame
    *
    * Row frames: are based on physical offsets from the position of the current input row. We can use <value> preceding
    *             and <value> FOLLOWING to describe the number of rows appear before and after the current input row.
    *             You can also use CURRENT ROW to describe boundary. So the size of frame is fixed. for example:
     *             The frame (ROWS BETWEEN 1 precding AND 1 FOLLOWING) always has three rows
    *
    *
    * RANGE frame: are based on logical offsets from the position of the current input row. A logical offset is the
    *             difference between the value of ther ordering expression of the current input row and the value of
    *             that same expression of the boundary row of the frame. The size of frame are different based on the
    *             value of current row.
    * */
    val rowFrameWindow=Window.partitionBy($"deptno").orderBy($"sal".desc).rowsBetween(Window.currentRow,Window.unboundedFollowing)
    val lastOfFrame=last($"sal").over(rowFrameWindow)
    empDF.select($"*",lastOfFrame as "last_val").show()
    /*Now the last val is correct*/
  }

  /**********************************************************************************************************
    * ******************************************** 4.5.6.7 miscellaneous functions ***************************************
    *********************************************************************************************************/
  def DSMiscellaneousOperations(spark:SparkSession):Unit={
    import spark.implicits._
/* Spark Sql supports also many other functions:
 * - crc32(e:Column):bigInt : Calculates the cyclic redundancy check value (CRC32) of a binary/String column and returns the
 *                            value as a bigint. It often used for data integrity checking
 * - md5(e:Column):String : Calculates the MD5 digest of a binary/String column and returns the value as a 32
 *                          character hex string, or NULL if the argument was NULL
 * - sha1(e:Column):String : Calculates the SHA-1 digest of a binary/String column and returns the value as a 40 character
 *                           hex string.
 * - sha2(e:Column,numBits:Int) : Calculates the SHA-2 family of hash functions of a binary/String value and returns
 *                                the value as a hex string. NumBits controls the number of bits in the message
 *                                disgest. numBits - one of 224, 256, 384, or 512.
 *                                */

    val df=List("I love","eat","apple","and","orange").toDF("word")
    df.show()
    df.select($"word",crc32($"word")).show()
    df.select($"word",md5($"word")).show()
    df.select($"word",sha1($"word")).show()
    df.select($"word",sha2($"word",512)).show()




  }

  def generateDf(names:List[String],items:List[String],size:Int):List[crossTab]={
    var loopSize=size
    val random=scala.util.Random
    var result:List[crossTab] = List[crossTab]()

    while(loopSize>0){
      val name_indice=random.nextInt(names.length)
      val item_indice=random.nextInt(items.length)
      val element=crossTab(loopSize,names(name_indice),items(item_indice))
      result=element::result
      loopSize=loopSize-1
    }
    return result
  }

  /**********************************************************************************************************
    * ******************************************** 4.5.6.8 User define functions ****************************
    *********************************************************************************************************/
  def DSUDFOperations(spark:SparkSession):Unit={
    import spark.implicits._

    val empDF = spark.createDataFrame(Seq(
      (7369, "SMITH", "CLERK", 7902, "17-Dec-80", 800, 20, 10),
      (7499, "ALLEN", "SALESMAN", 7698, "20-Feb-81", 1600, 300, 30),
      (7521, "WARD", "SALESMAN", 7698, "22-Feb-81", 1250, 500, 30),
      (7566, "JONES", "MANAGER", 7839, "2-Apr-81", 2975, 0, 20),
      (7654, "MARTIN", "SALESMAN", 7698, "28-Sep-81", 1250, 1400, 30),
      (7698, "BLAKE", "MANAGER", 7839, "1-May-81", 2850, 0, 30),
      (7782, "CLARK", "MANAGER", 7839, "9-Jun-81", 2450, 0, 10),
      (7788, "SCOTT", "ANALYST", 7566, "19-Apr-87", 3000, 0, 20),
      (7839, "KING", "PRESIDENT", 0, "17-Nov-81", 5000, 0, 10),
      (7844, "TURNER", "SALESMAN", 7698, "8-Sep-81", 1500, 0, 30),
      (7876, "ADAMS", "CLERK", 7788, "23-May-87", 1100, 0, 20)
    )).toDF("empno", "ename", "job", "mgr", "hiredate", "sal", "comm", "deptno")




    /* Spark SQL allows user-defined functions (UDFs) and user-defined aggregation functions (UDAFs). Both
     * UDFs and UDAFs perform custom computations on a dataset.
     * - UDF performs custom computation one row at a time and returns a value for each row.
     * - UDAF applies custom aggregation over groups of rows.
     * UDFs and UDAFs can be used just like the built-in functions after they have been registered with Spark SQL.*/

 /***************************************** 4.5.6.8.1 User define functions(UDFs) *****************************/
     /* To use a udf, you need to do 3 things
     * - 1. define your function(a code bloc which can be called in another scala script. It can be a class, object,
     *      a method, a function, etc.)
     * - 2. Register your function to spark session.
     * - 3. Use the registered function in a allowed method
     *
     * In the following example, we define a fucntion which calculate the net salary*/
        val taxRate:Double=0.15
    //You need to specify the argument type explicitlly, otherwise it wont work
    spark.udf.register("getNetSal",(sal:Int,taxRate:Double)=>getNetSal(sal,taxRate))
    // lit function is for adding literal values as column
    val taxDF=empDF.withColumn("taxRate",lit(taxRate))
    //taxDF.show()
    taxDF.withColumn("netSal",expr("getNetSal(sal,taxRate)")).show()

    /* It's also possible to use udf in filter*/

    /****************************** 4.5.6.8.3 User define aggregation functions(UDAFs) *****************************/

     /* In order to write a UDAF, we need to extend class UserDefinedAggregateFunctions and overwrite follwoing methods
      * - initialize: On a given node, this method is called once for each group (after groupBy)
      * - update: For a given group, spark will call “update” for each input record of that group.
      * - merge: if the function supports partial aggregates, spark might (as an optimization) compute partial
      *          result and combine them together.
      * - evaluate: Once all the entries for a group are exhausted, spark will call evaluate to get the final result.
      *
      * The execution order can vary in the following two ways:
      *    - 1. No partial aggregates (or combiner)
      *         initialize->update->update->...->evaluate No merge needed
      *    - 2. Supports partial aggregates. You can have merger between groups, and merger of merger return value
      *         And finally, use evaluate to return the final value
      *         initialize->update->update->...->merge-> merge01   ->merge012
      *         initialize1->update1->update1->...->merge1->merge01->merge012
      *         initialize2->update2->update2->...         ->merge2->merge012
      *
      * I use the class GeometricMean and CustomArthMean to illustrate how to write UDAFs, */

     //decalre and register UDAFs
    val geometricMean=new GeometricMean
    spark.udf.register("gm",geometricMean)


    val customArthMean=new CustomArthMean
    spark.udf.register("am",customArthMean)

    val ids=spark.sqlContext.range(1,20)
   // ids.printSchema()
   // println(s"ids value: ${ids.collect().mkString(",")}")
    val df=ids.select($"id",$"id" % 3 as "group_id")
    df.orderBy($"group_id".desc).show()

    // We usually use aggregation fucniton after groupBy, but it also works without
    df.groupBy("group_id").agg(expr("gm(id) as GeometricMean")).show()
    df.groupBy("group_id").agg(expr("am(id) as ArthmeticMean")).show()
    df.agg(expr("am(id) as ArthmeticMean")).show()

  }

  def getNetSal(sal:Int,taxRate:Double):Double={

    return sal*(1.00-taxRate)
  }

  case class crossTab(id:Int,name:String,item:String)
  case class NumTest(doubleNum:Double,logNum:Int,angleNum:Int)
  case class Nasdaq(clientID:Int,name:String,symbol:String,lastSale:String,marketCapLabel:String,marketCapAmount:Long,
                    ipoYear:String,sector:String,industry:String,summaryQuote:String)
}
