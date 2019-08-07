package org.pengfei.Lesson20_GPFS_Stats

import java.sql.Timestamp
import java.time._

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

object Lesson20_GPFS_Stats {

  def main(args:Array[String]):Unit={
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark=SparkSession.builder().master("local[2]").appName("Lesson20_GPFS_Stats").getOrCreate()

    spark.udf.register("getDate", (arg: Long)=>getDate(arg))
    spark.udf.register("getFileName", (arg:String)=>getFileName(arg))
    spark.udf.register("getParentDir",(arg:String)=>getParentDir(arg))
    spark.udf.register("getExtention",(arg:String)=>getExtention(arg))
    spark.udf.register("getSize",(arg:Long)=>getSize(arg))
    spark.udf.register("buildID",(a1:String,a2:Long)=>buildID(a1,a2))
    spark.udf.register("getUtec",(arg:String)=>getUtec(arg))
    //val createParentDirColUDF = udf(createParentDirCol(_:DataFrame,_:String, _: String))
    val createParentDirColUDF: UserDefinedFunction = spark.udf.register("createParentDirCol",(a1:DataFrame,a2:String,a3:String)=>createParentDirCol(a1,a2,a3))

    /************************************************************************************************
      * ******************************* Lesson20 GPFS Stats *********************************
      * ******************************************************************************************/

      /* In this lesson, we will learn how to use spark sql to analyse the statistic of a file system metric. The data
      * is the extraction (first 30000 lines) from a real file system. */
    val inputFile="/DATA/data_set/spark/basics/Lesson20_GPFS_Stats/gpfs_stats_sample.fist"

    val schema = StructType(Array(
      StructField("Blocks", LongType, true),
      StructField("Perms", LongType, true),
      StructField("nlinks", IntegerType, true),
      StructField("Uid", LongType, true),
      StructField("Gid", LongType, true),
      StructField("Size", LongType, true),
      StructField("Mtime", LongType, true),
      StructField("Atime", LongType, true),
      StructField("Ctime", LongType, true),
      StructField("Name", StringType, true)))

    val df = spark.read.format("com.databricks.spark.csv").option("delimiter", ":").schema(schema).load(inputFile)

    df.show(5)

    /************************************* 20.1 Data pre-processing *************************************/

    val parsedDf = df.withColumn("DataType",  expr("substring(Perms, 1, length(Perms)-4)"))
      .withColumn("ACL", expr("substring(Perms, length(Perms)-3,length(Perms))")).drop("Perms")
      .withColumn("FileName",expr("getFileName(Name)"))
      .withColumn("ParentDir",expr("getParentDir(Name)")).drop("Mtime").drop("Ctime")
      .withColumn("Extention",expr("getExtention(Name)"))
      .withColumn("FileID",expr("buildID(FileName,Size)"))
      .withColumn("Utec",expr("getUtec(Name)"))
      .withColumn("LastAccessTime",expr("getDate(Atime)"))

    parsedDf.show(5)
    parsedDf.cache()



    /*********************************** 20.2 Working with time  *****************************/

/* We have several time functions in Spark:
* - current_date: Gives current date as a date column. (e.g. 2019-05-23)
* - current_timestamp: Gives current time as a time stamp column. (e.g. 2019-05-23 10:37:19.585)
* - date_format: It creates a Column with DateFormatClass binary expression.
* - to_date(col(String))-> date: Converts column to date type(with an optional date format)
* - to_timestamp: Converts column to timestamp type (with an optional timestamp format)
* - unix_timestamp: Converts current or specified time to Unix timestamp (in seconds)
* - window: Generates time windows(i.e. tumbling, sliding and delayed windows)
* */

     // workingWithTime(spark)

    /*********************************** 20.3 Finding duplicate  *****************************/

   // findingDuplicate(parsedDf,10000L)

    /************************************ 20.4 Basic stats **********************************/

    // basicStats(parsedDf)

    /************************************* 20.5 Get all data not accessed since one year*******************/
     getOneYearOldData(parsedDf)
  }

  /*********************************** 20.2 Working with time  *****************************/
  def workingWithTime(spark:SparkSession):Unit={
    import spark.implicits._
    /*************************************** 20.2.1 current_date *****************************/
    val dateDf=spark.range(1).select(current_date().as("date"))
    dateDf.show(false)

    /*************************************** 20.2.2 current_timestamp *****************************/
    val timeStampDf=spark.range(1).select(current_timestamp())
    timeStampDf.show(false)

    /*************************************** 20.2.3 date_format *****************************/
    val c=date_format($"date","dd/MM/yyyy")
    import org.apache.spark.sql.catalyst.expressions.DateFormatClass
    val dfc = c.expr.asInstanceOf[DateFormatClass]
    println(dfc.prettyName)
    println(dfc.numberedTreeString)

    //show only the year
    dateDf.select(date_format($"date","y")).show()

    //change the date with the defined date_format c.
    dateDf.select(c).show()

    /*************************************** 20.2.4 to_date *****************************/
    val toDateDf=dateDf.select(to_date($"date","yyyy/MM/dd").as("to_date"))
    toDateDf.show(5)

   /**************************************  20.2.5 to_timestamp ************************/
    val toTimestampDf=dateDf.select(to_timestamp($"date","dd/MM/yyyy").as("to_timestamp"))
    toTimestampDf.show(1)

    /*get year, month, day*/

    val yearDf=toTimestampDf.select(year($"to_timestamp")).distinct()
    yearDf.show(1)

    val monthDf=toTimestampDf.select(month($"to_timestamp")).distinct()
    monthDf.show(1)

    val dayDf=toTimestampDf.select(dayofyear($"to_timestamp")).distinct()
    dayDf.show(1)


    /************************************  20.2.6 unix_timestamp ***********************/
    val unixTimeDf=dateDf.select(unix_timestamp($"date","MM/dd/yyyy").as("unix_timestamp"))
    unixTimeDf.show(1)

    //We can't use the function such as year, month, dayofyear with the digit of unix timestamp
    /*val yearDf=unixTimeDf.select(year($"unix_timestamp")).distinct()
    yearDf.show(1)

    val monthDf=unixTimeDf.select(month($"unix_timestamp")).distinct()
    monthDf.show(1)

    val dayDf=unixTimeDf.select(dayofyear($"unix_timestamp")).distinct()
    dayDf.show(1)*/

    /************************************  20.2.7 windows ***********************/
    /* What are windows and what are they good for?
    *
    * Consider the example of a traffic sensor that counts every 15 seconds the number of vehicles passing a certain
    * location. The resulting stream could look like:
    * 9, 6, 8, 4, 7, 3, 8, 4, 2, 1, 3, 2(1st element in the stream) ->
    *
    * If you would like to know, how many vehicles passed that location, you would simply sum the individual counts.
    * However, the nature of a sensor stream is that it continuously produces data. Such a stream never ends and it
    * is not possible to compute a final sum that can be returned. Instead, it is possible to compute rolling sums,
    * i.e., return for each input event an updated sum record. This would yield a new stream of partial sums like:
    * 57, 48, 42, 34, 30, 23, 20, 12, 8, 6, 5, 2
    *
    * However, a stream of partial sums might not be what we are looking for, because it constantly updates the
    * count and even more important, some information such as variation over time is lost. Hence, we might want to
    * rephrase our question and ask for the number of cars that pass the location every minute. This requires us to
    * group the elements of the stream into finite sets, each set corresponding to sixty seconds. This operation is
    * called a "tumbling windows operation". We can have a tumbling window like:
    * [9,6,8,4], [7,3,8,4], [2,1,3,2] -> for each window we have a sum 27, 22, 8.
    *
    * Tumbling windows discretize a stream into non-overlapping windows. For certain applications it is important
    * that windows are not disjunct because an application might require smoothed aggregates. For example, we can
    * compute every thirty seconds the number of cars passed in the last minute. Such windows are called "sliding windows".
    *
    * */

    val levels = Seq(
      // (year, month, dayOfMonth, hour, minute, second)
      ((2012, 12, 12, 12, 12, 12), 5),
      ((2012, 12, 12, 12, 12, 14), 9),
      ((2012, 12, 12, 13, 13, 14), 4),
      ((2016, 8,  13, 0, 0, 0), 10),
      ((2017, 5,  27, 0, 0, 0), 15)).
      map { case ((yy, mm, dd, h, m, s), a) => (LocalDateTime.of(yy, mm, dd, h, m, s), a) }.
      map { case (ts, a) => (Timestamp.valueOf(ts), a) }.
      toDF("time", "level")

    levels.show(5)

    val timeWindow=levels.select(window($"time","5 seconds"),$"level")

    timeWindow.show(5,false)

    timeWindow.printSchema

    val sums=timeWindow.groupBy($"window")
      .agg(sum("level").as("level_sum"))
      .select($"window.start",$"window.end",$"level_sum")

    sums.show(false)
  }



  /*********************************** 20.3 Finding duplicate  *****************************/

/**
  * This functions filter the data by its size first, then find duplicate of the filtered data. We use to column FileID
  * to distinguish the data, it's built based on the file name and size. So it's not very accurate way to detect
  * duplicated data. */
 def findingDuplicate(parsedDf:DataFrame,size:Long):Unit={
   val spark=parsedDf.sparkSession
   import spark.implicits._
   val duplicatedFile=parsedDf.filter($"Size">size) // only check files has a minimun size
     .groupBy($"FileID") // group files by their ID
     // After groupby, use aggregation function to calculate total size and count for each file
     .agg(sum("Size").alias("fileSize"), count($"FileID").alias("count"))
     .filter($"count">1) // select the file which has more than 1 count
     .orderBy($"fileSize".desc) // order the data frame by file size

   duplicatedFile.show(5,false)

   val duplicatedFileWithParentDir=duplicatedFile.join(parsedDf,Seq("FileID"),joinType = "inner")
     .select("FileID","fileSize","count","ParentDir","LastAccessTime").orderBy("FileID")

   duplicatedFileWithParentDir.show(5,false)
 }

  /******************************** 20.4 Basic Stats *************************************/

  /**
    * In this function, we will do some basic stats such as */
  def basicStats(parsedDf: DataFrame):Unit={
    val spark:SparkSession=parsedDf.sparkSession
    import spark.implicits._

    //Total fastq file count and size
    val allFastqs=parsedDf.filter($"Extention"==="fastq")

    // allFastqs.show(5)
    val allFastqsNum=allFastqs.count()
    println(s"All fastq file number count: ${allFastqsNum}")

    /* utec02 fastq*/
    val utec02Fastq=allFastqs.filter($"Utec".startsWith("pt2"))
    val utec02AllFastqsNum=utec02Fastq.count()
    println(s"All fastq file number count of Utec 02: ${utec02AllFastqsNum}")


    /* Get all gpfs file size and fastq size. */
    val allSize=parsedDf.agg(sum("Size")).first.get(0)
    val totalFastqSize=allFastqs.agg(sum("Size")).first.get(0)
    val utec02FastqSize=utec02Fastq.agg(sum("Size")).first.get(0)

    val HAllFastqSize=getSize(totalFastqSize.asInstanceOf[Number].longValue)
    val HAllSize=getSize(allSize.asInstanceOf[Number].longValue)
    val HUtec02FastqSize=getSize(utec02FastqSize.asInstanceOf[Number].longValue)

    println(s"All file size in GPFS: ${HAllSize}")
    println(s"All fastq file size in GPFS: ${HAllFastqSize}")
    println(s"All fastq file size of UTEC02 in GPFS: ${HUtec02FastqSize}")


    /* Get all file extention, and count total */
    val distinctFileType=parsedDf.select("Extention").distinct().filter(length($"Extention")<10)
    distinctFileType.show(5,false)
    distinctFileType.count

    /* Count file number and size of each extention type*/
    val fileTypeCount=parsedDf.filter(length($"Extention")<10).groupBy("Extention").count().orderBy($"count".desc)
    fileTypeCount.show(10)


    val fileTypeSortBySize=parsedDf.groupBy($"Extention").agg(expr("sum(Size) as TotalSize")).orderBy($"TotalSize".desc)
      .withColumn("HTotalSize",expr("getSize(TotalSize)"))

    fileTypeSortBySize.show(10)

  }

  /**************************** 20.5 Get all data not accessed since one year *************************/
  def getOneYearOldData(parsedDF:DataFrame):Unit={
    val spark:SparkSession=parsedDF.sparkSession
    import spark.implicits._
    parsedDF.show(1)
    val allDataCount=parsedDF.count()
    /*There are two options based on our data set
    * 1. As we have the column Atime which is a unix timestamp with Long type, we can use the current time-oneYear unix
    * time length to get the last year max value. And all value < the max value is older than one year*/
    // one year length in unix timestamp
    val oneYearLength=31536000L
    // current time
    val ctime:Long= Instant.now.getEpochSecond
    val oneYearMax=ctime-oneYearLength
    /* 1533686400
     Is equivalent to 08/08/2018*/
    /* test the value*/
    println(s"ctime is ${ctime}, last year is ${oneYearMax}")

     // Filter all data older than one year max
    val oldDataDf=parsedDF.filter($"Atime"<oneYearMax)
    val oldDataCount=oldDataDf.count()
    println(s"Old data number count: $oldDataCount, all data count: $allDataCount")
    oldDataDf.show(5)


    /* 2. We can use the column LastAccessTime which has string format date, then we can convert the string to time
     *    stamp. Then we use the build in function year, day of year to get day older than one year.
     *
     *    For example, we have date in 2016, 2017,2018, 2019. The current date is 2019-08-08 (day of year is 220),
     *    Step1, we know 2016, 2017 < 2019-1. So they must be older than one year
     *    Step2 , for date in 2018, if dayofyear of the day < 220, we know it older than one year
     *
     *    */

    /*val dateDf=parsedDF.withColumn("timestamp",to_timestamp($"LastAccessTime","yyyy-MM-dd"))
    //dateDf.printSchema()
    dateDf.show(1,false)

    /* Step1. first we filter the date < current year-1*/
    val currentYear=Year.now.getValue

    val oldData1=dateDf.filter(year($"timestamp")<currentYear-1)
    val old1Count=oldData1.count()
    /* Step2. We filter teh the date = current_year-1 and dayofYear > current dayofYear*/
    val currentDayofYear=Calendar.getInstance.get(Calendar.DAY_OF_YEAR)
    println(s"Current day of year: ${currentDayofYear}")
    val oldData2=dateDf.filter((year($"timestamp")===currentYear) && (dayofyear($"timestamp")<100))
    val oldAllData=oldData1.union(oldData2)
    val old2Count=oldData2.count()
    val oldDataCount=oldAllData.count()
    println(s"Old data number count: $oldDataCount, all data count: $allDataCount")
    println(s"Old1 count: $old1Count, Old2 count: $old2Count")
    oldAllData.show(1)*/
  }


/**************Helper class for curate raw data to the new data frame ****************************/
def createParentDirCol(fullDf:DataFrame,colName:String,colVaule:String):List[String]={
  val spark=fullDf.sparkSession
  import spark.implicits._

  fullDf.filter(col(colName)===colVaule).select("ParentDir").map(_.getString(0)).collect.toList
}


  def getSize(rawSize:Long): String ={
    val unit:Array[String]=Array("B","KB","MB","GB","TB")
    var index=0
    var tmpSize:Long=rawSize
    while(tmpSize>=1024){
      tmpSize=tmpSize/1024
      index+=1
    }
    return tmpSize+unit(index)
  }

  def getFileName(fullPath:String):String={
    val fileName=fullPath.substring(fullPath.lastIndexOf("/")+1)
    return fileName
  }
  def getParentDir(fullPath:String):String={
    val index=fullPath.lastIndexOf("/")
    if(index>0&&index<fullPath.length){
      val parentDir=fullPath.substring(0,fullPath.lastIndexOf("/"))
      return parentDir}
    else return "None"
  }
  def getDateInMillis(date:String):Long={
    val format=new java.text.SimpleDateFormat("m/dd/yyyy")
    val time=format.parse(date).getTime()/1000
    return time
  }

  def getDate(rawDate:Long):String={
    val timeInMillis = System.currentTimeMillis()
    val instant = Instant.ofEpochSecond(rawDate)
    val zonedDateTimeUtc= ZonedDateTime.ofInstant(instant,ZoneId.of("UTC"))
    val zonedDateTimeCet=ZonedDateTime.ofInstant(instant,ZoneId.of("CET"))
    zonedDateTimeUtc.toString
  }

  def getExtention(fileName:String):String={
    val index=fileName.lastIndexOf(".")
    if(index>0)return fileName.substring(index+1)
    else return "None"
  }

  def buildID(fileName:String,Size:Long):String={
    return fileName.concat(Size.toString)
  }

  def getUtec(fullPath:String):String={
    if(fullPath.contains("pt")&&fullPath.length>3){
      return fullPath.substring(0,3)
    }
    else return "Others"
  }

}
