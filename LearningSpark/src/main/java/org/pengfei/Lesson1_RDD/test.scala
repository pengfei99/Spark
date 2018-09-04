package org.pengfei.Lesson1_RDD

object test {

  def main(args:Array[String]):Unit={
    val fileName="pliu.txt"
    val badfn="pliu.txt.bkp"
    println(getExtention(fileName))
    println(getExtention(badfn))
  }

  def getExtention(fileName:String):String={
     val index=fileName.lastIndexOf(".")
    if(index>0)return fileName.substring(index+1)
    else return "None"
  }


}
/*
*
*
*
*
* import org.apache.spark.sql.types._
val inputFile = "/test_data/bioaster-2018-03-06.fist"


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


*
*
*
*
* import java.time.{Instant,ZoneId,ZonedDateTime}

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

spark.udf.register("getDate", (arg: Long)=>getDate(arg))
spark.udf.register("getFileName", (arg:String)=>getFileName(arg))
spark.udf.register("getParentDir",(arg:String)=>getParentDir(arg))
spark.udf.register("getExtention",(arg:String)=>getExtention(arg))
spark.udf.register("getSize",(arg:Long)=>getSize(arg))
*
*
*
*
*
* val result = df.withColumn("DataType", expr("substring(Perms, 1, length(Perms)-4)")).withColumn("ACL", expr("substring(Perms, length(Perms)-3,length(Perms))")).drop("Perms")
    .withColumn("FileName",expr("getFileName(Name)"))
    .withColumn("ParentDir",expr("getParentDir(Name)")).drop("Mtime").drop("Ctime")
    .withColumn("Extention",expr("getExtention(Name)"))
result.show(5)
//result.count()
*
*
* //Total fastq file count and size
val allFastqs=result.filter($"FileName".endsWith(".fastq"))

allFastqs.count()
val totalSize=allFastqs.agg(sum("Size")).first.get(0)
val HSize=getSize(totalSize.asInstanceOf[Number].longValue)
*
*
* val fastqCountByDir=allFastqs.groupBy($"ParentDir").count().orderBy($"count".desc)
//fastqCountByDir.count()
//fastqCountByDir.show(5)

val fastqSizeByDir=allFastqs.groupBy($"ParentDir").agg(expr("sum(Size) as TotalSize")).orderBy($"TotalSize".desc)
                   .withColumn("HTotalSize",expr("getSize(TotalSize)"))
fastqSizeByDir.show(10)


val fileTypeSortByNum=result.groupBy($"Extention").count().orderBy($"count".desc)
//fileTypeSortByNum.show(10)

val fileTypeSortBySize=result.groupBy($"Extention").agg(expr("sum(Size) as TotalSize")).orderBy($"TotalSize".desc)
                       .withColumn("HTotalSize",expr("getSize(TotalSize)"))
fileTypeSortBySize.show(10)


val duplicatedFile=result.groupBy($"FileName").count().filter($"count">1).orderBy($"count".asc)
duplicatedFile.show(20)
* */