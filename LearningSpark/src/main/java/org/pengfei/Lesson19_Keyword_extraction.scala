package org.pengfei

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}

object Lesson19_Keyword_extraction {

  def main(args:Array[String]):Unit= {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val spark = SparkSession.builder().appName("Lesson19_Keyword_extraction").master("local[2]").getOrCreate()
    import spark.implicits._

    /************************************************************************************************
      * ******************************* Lesson19 Keyword extraction *********************************
      * ******************************************************************************************/

    /* In this lesson, we will learn how to extract key word from articles, since they provide a concise representation
    of the article’s content. Keywords also play a crucial role in locating the article from information retrieval
    systems, bibliographic databases and for search engine optimization. Keywords also help to categorize the article
    into the relevant subject or discipline. We will use NLP techniques on a collection of articles to extract keywords


    About the dataset
    The dataset which we use is from Kaggle (https://www.kaggle.com/benhamner/nips-papers/home). Neural Information
    Processing Systems (NIPS) is one of the top machine learning conferences in the world. This dataset includes the
    title and abstracts for all NIPS papers to date (ranging from the first 1987 conference to the current 2016 conference).

The nips-papers.csv contains the following columns:
- id,
- year,
- title,
- event_type,
- pdf_name : the name of the pdf file
- abstract : abstract of the paper
- paper_text : paper main content body

In this lesson, we focus on the concept of keyword extraction, so we only use abstracts of these articles to extract
keywords, because full text search is time consuming

    */

    /************************** 19.0 key stages of keyword extraction ************************/

    /*
1. Text pre-processing
   a. noise removal
   b. normalisation

2. Data Exploration
   a. Word cloud to understand the frequently used words
   b. Top 20 single words, bi-grams and tri grams

3. Convert text to a vector of word counts

4. Convert text to a vector of term frequencies

5. Sort terms in descending order based on term frequencies to identify top N keywords
*/

    /************************ 19.1 Preliminary text exploration ***********************************/
    /* The origin papers.csv is exported with pandas with default delimiter ",", which cause conflit
    * in the main text column which has many ",". If we read it with spark, we have many problems, but
    * if we read with pandas, it works, so we read the papers.csv and select the "id, year, title, abstract
    * columns and export to csv(abstract1.csv) with "|" as delimiter.
    * */

    val filePath="/DATA/data_set/spark/pyspark/Lesson2_Keyword_Extraction/abstract1.csv"
    val rawDf = spark.read
      .option("inferSchema", true)
      .option("header",true)
      .option("nullValue"," ")
      .option("encoding", "UTF-8")
      .option("delimiter","|")
      .csv(filePath)

    //rawDf.show(5)

    /* we select only id, year, title and abstract as columns, then we merge the title and abstract to a new column
    abstract1 */

   val rawCleanDf=replaceSpecValue(rawDf,Array("abstract"),"Abstract Missing","")
   //rawCleanDf.show(5)

    val df=rawCleanDf.withColumn("abstract1",concat($"title",lit(" "),$"abstract"))
      .drop($"title")
      .drop($"abstract")

    df.show(5)

    val totalWordCountDf=df.withColumn("word_count",size(split($"abstract1"," ")))
    totalWordCountDf.show(5)

    val wordsExplodeDf=df.withColumn("words",explode(split($"abstract1"," ")))

    wordsExplodeDf.show(5,false)

    val wordOccurrence=wordsExplodeDf.groupBy("words").count().sort(desc("count"))

    wordOccurrence.show(10,false)
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

}
