package org.pengfei.Lesson14_Understanding_Wiki_With_Latent_Semantic_Analysis

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.{RegexTokenizer, StopWordsRemover, Tokenizer}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Lesson14_Spark_Xml_Parsing {

  /****************************************************************************************************************
    * ******************************** 14.A Spark xml parsing ****************************************************
    * **********************************************************************************************/


  def main(args:Array[String]):Unit={
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val spark=SparkSession.builder().appName("Lesson14_Spark_Xml_Parsing").master("local[2]").getOrCreate()
    import spark.implicits._

    val filePath="/DATA/data_set/spark/basics/Lesson14_Latent_Semantic_Analysis/mini-wikidump.xml"

    /* As we have bugs in parsing the xml files, so here we use another xml parsing techinque to parse the wiki
    * xml dump.
    *
    * The lib which we use here is from databricks. You can find more info here (https://github.com/databricks/spark-xml)
    *
    * */

    /*********************************14.A.1 Read xml as dataframe *****************************************/
    /* There are few key options to read the xml files
    * - path : file location (e.g. local fs, hdfs, s3)
    * - rowTag : The row tag of your xml files to treat as a row. For example, in this xml
    *            <books> <book><book> ...</books>, the appropriate value would be book. Default is ROW.
    *            At the moment, rows containing self closing xml tags are not supported.
    * - rootTag : The root tag of your xml files to treat as the root. in this xml <books> <book><book> ...</books>,
    *             the appropriate value would be books. Default is ROWS
    * - nullValue : The value to write null value. Default is string null. when this is null, it does not write
    *               attributes and elements for fields.
    * - valueTag: The tag used for the value when there are attributes in the element having no child. Default is _VALUE.
    *
    * */

    val df=spark.read.format("com.databricks.spark.xml").option("rowTag","page").load(filePath)

    // df.show(1,false)
    // df.printSchema()
    // println(s" row number : ${df.count()}")

    /* The xml file has the following structure :
    * <page>
    *    <title>...</title>
         <ns>...</ns>
         <id>...</id>
         <revision>
           <id>546483579</id>
           <parentid>524214820</parentid>
           <timestamp>2013-03-23T06:27:20Z</timestamp>
           <contributor>
              <username>Addbot</username>
              <id>6569922</id>
           </contributor>
           <minor/>
           <comment> ... </comment>
           <model>wikitext</model>
           <format>text/x-wiki</format>
           <text xml:space="preserve" bytes="430">...</text>
           <sha1>apzvvk61z15qfk90rh4y7nkhu8rzay2</sha1>
         </revision>
    *  </page>
    *
    *  The output ds has the following schema:
    *
    *  root
        |-- id: long (nullable = true)
        |-- ns: long (nullable = true)
        |-- revision: struct (nullable = true)
        |    |-- comment: string (nullable = true)
        |    |-- contributor: struct (nullable = true)
        |    |    |-- id: long (nullable = true)
        |    |    |-- username: string (nullable = true)
        |    |-- format: string (nullable = true)
        |    |-- id: long (nullable = true)
        |    |-- minor: string (nullable = true)
        |    |-- model: string (nullable = true)
        |    |-- parentid: long (nullable = true)
        |    |-- sha1: string (nullable = true)
        |    |-- text: struct (nullable = true)
        |    |    |-- _VALUE: string (nullable = true)
        |    |    |-- _bytes: long (nullable = true)
        |    |    |-- _space: string (nullable = true)
        |    |-- timestamp: string (nullable = true)
        |-- title: string (nullable = true)
    * */

    /* We could notice the first level son of page become columns, as it's a dataset, we can access also the
    * second/third/... level of sons */
    val test=df.select("title","revision")
    // test.show(1,false)
    // test.printSchema()

    /* Create a new column text, which is the second level son of page which represent text, note that
    * <text> has parameters (e.g. space, bytes), with $"revision.text"*/
    val text=test.withColumn("fullText",$"revision".getField("text"))
     /*text.select("fullText").show(1,false)*/

    /* Another way to get the second level son*/
    val text1=test.withColumn("fullText",$"revision.text")
    /*text1.select("fullText").show(1,false)*/

    /* Get the value of space or bytes of the <text xml:space="",bytes="">...</text> */
    val spaceAndByptes=test.withColumn("space",$"revision.text._space")
      .withColumn("bytes",$"revision.text._bytes")
    /*spaceAndByptes.select("space","bytes").show(1,false)*/

    /* With the following code, we can get pure text value without space and bytes parameters values*/
    val pureTextValue= test.withColumn("text",$"revision.text._VALUE")
    pureTextValue.select("text").show(1,false)

     TextToTerm(spark)
  }

  def TextToTerm(spark:SparkSession):DataFrame={
    val text=spark.createDataFrame(Seq(
      (0,"'''[[Discrete geometry]]''' or '''combinatorial geometry''' may be loosely defined as study of geometrical objects and properties that are [[discrete mathematics|discrete]] or [[combinatorial]], either by their nature or by their representation; the study that does not essentially rely on the notion of [[continuous function|continuity]].\n{{Commons cat|Discrete geometry}}\n\n[[Category:Geometry]]\n[[Category:Discrete mathematics]]")


    )).toDF("id","text")
    // The normal tokenizer takes also the special characters such as ''', [,], it only uses space
    // to split words
    val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("terms")

    // The regexTokenizer can take regular expression to filter tokens which matches
    val regexTokenizer = new RegexTokenizer()
      .setInputCol("text")
      .setOutputCol("terms")
      .setPattern("\\W")


    /*val tokenized = tokenizer.transform(text)
    tokenized.select("text","terms").show(1,false)*/


    val regexTokenized=regexTokenizer.transform(text)
    regexTokenized.select("text","terms").show(false)

    // add a stop words remover
    val remover= new StopWordsRemover()
      .setInputCol("terms")
      .setOutputCol("filteredTerms")

    val filteredTerms=remover.transform(regexTokenized)
    filteredTerms.select("terms","filteredTerms").show(1,false)
    filteredTerms
  }

}
