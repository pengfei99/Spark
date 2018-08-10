package org.pengfei.spark.application.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import java.time.{Instant,ZoneId,ZonedDateTime}

object GPFSStats {
def main(args:Array[String]): Unit = {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val spark = SparkSession.builder().
    master("local[2]"). //spark://10.70.3.48:7077 remote
    appName("GPFSStats").
    getOrCreate()
  //spark.conf.set("")
  import spark.implicits._

  val inputFile = "file:///DATA/data_set/spark/gpfs_stats_min"
  //val inputFile="hdfs://hadoop-nn.bioaster.org:9000/test_data/bioaster-2018-03-06.fist"
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
  //df.show(5)
  df.count()

  //df.filter($"Name".contains(".delta")).select($"Name",$"Blocks").groupBy($"Name".contains(".delta")).sum("Blocks").show(5)
  //val dir= df.select($"Perms",$"Name").distinct().filter($"Perms"===200040775)
  //dir.collect()
  /*1. transfrom Perms column into DataType and acl*/
  val result = df.withColumn("DataType", expr("substring(Perms, 1, length(Perms)-4)")).withColumn("ACL", expr("substring(Perms, length(Perms)-3,length(Perms))")).drop("Perms")

  /*2. transform Name into FileName and ParentDir*/
  /*3. transform Atime to ADate*/
  /*4. Uid to username*/

  /*Q1. list the numbers of fastq not compressed of each dir, for each target dir, give the list of all non compressed file Name*/

  /*Q2. Do Q1 only for data in UserData */

  /*Q3. find all duplicate data (same name, same size)*/

  /*Q4. Get data not accessed since 1 year*/

 val out= result.limit(5)

  //out.write.format("csv").option("header","true").save("file:///tmp/test.csv")
  //result.select($"DataType",$"Name").distinct().show(5)

  //get user 42968 file number
  //result.filter($"Uid"===42968).select($"Uid",$"Size").groupBy("Uid").count().show(5)

  //get user 42968 .fastq file size
  //  result.filter($"Uid"===42968).filter($"Name".contains(".fastq")).select($"Uid",$"Size").groupBy("Uid").sum("Size").show()

  // val userList= result.select($"Uid").distinct().rdd.map(r=>r(0)).collect()

  /*//get all user space usage size
  val spaceSize = result.select($"Uid", $"Size").groupBy("Uid").sum("Size")
  //create column
  val newdf = spaceSize.withColumn("SizeH", col("sum(Size)")).drop("sum(Size)")
  newdf.show(5)
  spark.udf.register("getSize", (arg1: Long) => getSize(arg1))

  val finaldf = newdf.withColumn("Size", expr("getSize(SizeH)"))
  finaldf.show(5)*/

  val convertDate=result.select($"Atime")
  spark.udf.register("getDate", (arg: Long)=>getDate(arg))
  spark.udf.register("getFileName", (arg:String)=>getFileName(arg))
  spark.udf.register("getParentDir",(arg:String)=>getParentDir(arg))
  val dateDF=convertDate.withColumn("ADate",expr("getDate(Atime)"))
  val tmpDF=out.withColumn("FileName",expr("getFileName(Name)"))
                  .withColumn("ParentDir",expr("getParentDir(Name)"))
  dateDF.show(5)
  tmpDF.show(5)


  tmpDF.groupBy($"FileName").count().sort($"count".desc).show()
  println(getDateInMillis("03/08/2017"))
  //tmpDF.write.format("csv").option("header","true").save("file:///tmp/fileName.csv")


  //newdf.withColumn("Size",getSize(("SizeH))
  /*for(user<-userList){
    println(user)
    result.filter($"Uid"===user).select($"Uid",$"Size").groupBy("Uid").sum("Size").show()
  }

}
}
}*/

}
  /*def main(args:Array[String]): Unit ={
    //val rawSize:Long= 6403617598L
    //getSize(rawSize)

    val rawDate=1494438629
    getDate(rawDate)
  }*/
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
  val parentDir=fullPath.substring(0,fullPath.lastIndexOf("/"))
  return parentDir
}
def getDate(rawDate:Long):String={
  val timeInMillis = System.currentTimeMillis()

  val instant = Instant.ofEpochSecond(rawDate)
  //instant: java.time.Instant = 2017-02-13T12:14:20.666Z
  val zonedDateTimeUtc= ZonedDateTime.ofInstant(instant,ZoneId.of("UTC"))
  //zonedDateTimeUtc: java.time.ZonedDateTime = 2017-02-13T12:14:20.666Z[UTC]
  val zonedDateTimeCet=ZonedDateTime.ofInstant(instant,ZoneId.of("CET"))
 /* println("Current time in milisecond"+ timeInMillis)
  println("Current time: "+Instant.ofEpochMilli(timeInMillis))
  println("Instant time :"+instant)
  println("UTC time :"+zonedDateTimeUtc)
  println("CET time :"+zonedDateTimeCet)*/
  zonedDateTimeUtc.toString
}
  def getDateInMillis(date:String):Long={
    val format=new java.text.SimpleDateFormat("m/dd/yyyy")
    val time=format.parse(date).getTime()/1000
    return time
  }
}

/*
*
* blocks perms nlinks uid gid size mtime atime ctime name ("name" is "name -> lname" when the object is a link)
blocks: 1 bloc for 1 mb in gpfs, if file < 50 oct, write direct in meta data, else in a sub block, for example a file of 1.5 mb, it will use 2 block and we lose 0.5mb of space.
perms: mode_t
atime, ctime, mtime: date Unix epoch based

blocks->0:perms->200100664:nlinks->1:uid->42968:gid->8000:size->140:mtime->1494438629:atime->1494438629:ctime->1494438629:name->pt2/ama/
*
* */


/*
*1515366180000
*1494438629
* val myUDf = udf((s:String) => Array(s.toUpperCase(),s.toLowerCase()))

val df = sc.parallelize(Seq("Peter","John")).toDF("name")

val newDf = df
  .withColumn("udfResult",myUDf(col("name")))
  .withColumn("uppercaseColumn", col("udfResult")(0))
  .withColumn("lowercaseColumn", col("udfResult")(1))
  .drop("udfResult")

newDf.show()
* */



/*
* +------+------+-----+----+----+----------+----------+----------+--------------------+--------+----+
|Blocks|nlinks|  Uid| Gid|Size|     Mtime|     Atime|     Ctime|                Name|DataType| ACL|
+------+------+-----+----+----+----------+----------+----------+--------------------+--------+----+
|     0|     1|42968|8000| 140|1494438629|1494438629|1494438629|pt2/ama/nanostrin...|   20010|0664|
|     0|     1|42968|8000| 140|1494438629|1494438629|1494438629|pt2/ama/nanostrin...|   20010|0664|
|     0|     1|42968|8000| 140|1494438629|1494438629|1494438629|pt2/ama/nanostrin...|   20010|0664|
|     0|     1|42968|8000| 140|1494438629|1494438629|1494438629|pt2/ama/nanostrin...|   20010|0664|
|     0|     1|42968|8000| 140|1494438629|1494438629|1494438629|pt2/ama/nanostrin...|   20010|0664|
+------+------+-----+----+----+----------+----------+----------+--------------------+--------+----+
*
* atime -> File access time
*          Access time shows the last time the data from a file was accessed – read by one of
*          the Unix processes directly or through commands and scripts.
*
* ctime -> File change time
*          ctime also changes when you change file's ownership or access permissions. It will
*          also naturally highlight the last time file had its contents updated.
*
* mtime -> File modify time
*          Last modification time shows time of the  last change to file's contents. It does
*          not change with owner or permission changes, and is therefore used for tracking
*          the actual changes to data of the file itself.
* */