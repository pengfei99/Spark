package org.pengfei.spark.application.example

import java.io.File

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}


/*
*
* Inverted Index is mapping of content like text to the document in
* which it can be found. Mainly used in search engines, it provides
* faster lookup on text searches i.e to find the documents where the
* search text occurs.
*
*
* Problem Statement:
* 1. Dataset contains Shakespeare's works split among many files
* 2. The output must contain a list of all words with the file in which
*    it occurs and the number of times it occurs
*
* */
object InvertedIndexShakespeare {

  case class wordFile(wordName:String,wordCount:Int,fileName:String)
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val fileDir="/home/pliu/Downloads/data_set/inverted-index-master/dataset/shakespeare"
    //get the file list
    val fileList=getFileList(fileDir)
    //fileList.foreach(file=>println("file://"+file.getAbsolutePath))

    val spark = SparkSession.builder().master("local").appName("InvertedIndexShakespeare").getOrCreate()
    import spark.implicits._

    /*******************************************
    *build wordCound df for all files in the directory and write df to parquet file
      * *********************************************/
    /*val wordCountDF=buildFullWordCountDataFrame(fileList,spark)
    val parquetFilePath="file:///home/pliu/Downloads/data_set/inverted-index-master/dataset/parquet"
    wordCountDF.write.parquet(parquetFilePath)*/

    /*
    * WE can use dataframe build in function to transform data
    *
    * */

    //wordCountDF.filter($"word_name"=!="/s").orderBy($"word_count".desc).show(10)

    /******************************************
      * Read parquet file and create  a view for sql query
      * *****************************/
   /* val wordCountDF=spark.read.parquet(parquetFilePath)
    wordCountDF.createOrReplaceTempView("wordCount")
    val popularWord=spark.sql("select * from wordCount where word_name <> ' ' ORDER BY word_count DESC")
    popularWord.show(20)*/
    //wordCountDF.orderBy($"word_count".desc).show(10)
   // wordCountDF.show(10)

    /*val testfilePath="file:///home/pliu/Downloads/data_set/inverted-index-master/dataset/shakespeare/0ws0110.txt"
    val testDF=wordCount(testfilePath,spark,"0ws0110.txt")
    testDF.orderBy($"word_count".desc).show(10)*/


  }

  def buildFullWordCountDataFrame(fileList:List[File],spark:SparkSession):DataFrame ={
    import spark.implicits._
    val sc = spark.sparkContext
    val sqlC=spark.sqlContext

    val schema = StructType(Array(
      StructField("word_name",StringType,false),
      StructField("word_count",IntegerType,false),
      StructField("file_name",StringType,false)
    ))

    var fullDf : DataFrame = sqlC.createDataFrame(sc.emptyRDD[Row],schema)
    var totalColumn:Long = 0
    for(file<-fileList){
      val fileName=file.getName
      val filePath="file://"+file.getAbsolutePath
      val wordDF=wordCount(filePath,spark,fileName)
      fullDf=fullDf.union(wordDF)
      totalColumn=totalColumn+wordDF.count()

    }
    /*println("Total row :"+totalColumn)
    println("Data frame row :"+ fullDf.count())*/
    return fullDf
  }

  def wordCount(filePath:String,spark:SparkSession,fileName:String):DataFrame= {
    import spark.implicits._
    val sc = spark.sparkContext
    val textFile = sc.textFile(filePath)
    //filter word.isEmpty eliminats white space words
    val wordCount = textFile.flatMap(line=>line.split(" ")).filter(word => !word.isEmpty).map(word=>(word,1)).reduceByKey((a,b)=>a+b)
    val wordDF=wordCount.map(atts=>wordFile(atts._1,atts._2.toInt,fileName)).toDF("word_name","word_count","file_name")
    return wordDF
  }

  def getFileList(fileDir:String):List[File]={
    val dir = new File(fileDir)
    if(dir.exists() && dir.isDirectory){
      val fileList=dir.listFiles().filter(file=>file.getName.startsWith("0ws")).toList
      return fileList
    }
    else null
  }

  def row(line : List[String], fileName : String):Row = {
    Row(line(0),line(1).toInt,fileName)
  }


}
