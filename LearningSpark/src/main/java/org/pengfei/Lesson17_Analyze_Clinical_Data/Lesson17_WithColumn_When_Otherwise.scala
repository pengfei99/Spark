package org.pengfei.Lesson17_Analyze_Clinical_Data

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType //for 'when'

object Lesson17_WithColumn_When_Otherwise {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val spark = SparkSession.builder().master("local[2]").appName("Lesson17_WithColumn_").getOrCreate()
    import spark.implicits._



    val df = spark.sparkContext.parallelize(Seq((4, "blah", 2), (2, "", 3), (56, "foo", 3), (100, null, 5)))
      .toDF("A", "B", "C")

    df.show()

    /*val newDf = df.withColumn("D", when($"B" === "", 0))

    newDf.show()*/

    /*val newDf=replaceSpecValue(df,Array("B"),"foo","ok")
    newDf.show()*/

    //remove a row which column B = ""
    val afterDelete=removeRowsWithSpecValue(df,"B","")
    afterDelete.show()

    val afterDeleteR=removeRowsWithSpecValues(df,"B",Array("","blah"))
    afterDeleteR.show()
  }

  def replaceSpecValue(rawDf:DataFrame,colNames:Array[String],specValue:String,newValue:String):DataFrame={
    /*Step 0 : cast all column to string*/
    val spark=rawDf.sparkSession
    import spark.implicits._
    val df=rawDf.select(rawDf.columns.map(c=>col(c).cast(StringType)):_*)

    /*Step 1 : transform spec value to null*/
    var result=df
    for(colName<-colNames){
      val newColName=colName+"_tmp"
      /* We must specify the when clause with .otherwise(value), if not it will replace all rows which do not satisfy
      * the when condition with null. In our case we use the origin value of the row otherwise(result(colName)).
      */
      result=result.withColumn(newColName, when(result(colName) === specValue, newValue).otherwise(result(colName))) //create a tmp col with digitnull
        .drop(colName) //drop the old column
        .withColumnRenamed(newColName,colName) // rename the tmp to colName
    }
    result
  }

  def removeRowsWithSpecValue(df:DataFrame,colName:String,specValue:String):DataFrame={
    val result=df.filter(!(df(colName)===specValue))
    result
  }

  def removeRowsWithSpecValues(df:DataFrame,colName:String,specValues:Array[String]):DataFrame={
    var result=df
    for(specValue<-specValues){
      result=result.filter(!(result(colName)===specValue))
    }
    result
  }
}
