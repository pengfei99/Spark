package org.pengfei.Lesson17_Analyze_Clinical_Data

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.collection.mutable.Map

object Lesson17_Compare_Two_Columns {
  def main(args:Array[String]):Unit= {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val spark=SparkSession.builder().appName("Lesson17_Compare_Two_Columns").master("local[2]").getOrCreate()
    import spark.implicits._
    val df = spark.sparkContext.parallelize(Seq(
      ("null","1"),
      ("cat13","cat13"),
      ("cat95","cat95"),
      ("0","0"),
      ("1","1"),
      ("0","1"),
      ("1","0"),
      ("1","null"),
      ("cat56","null"),
      ("null","cat40"),
      ("null","null")
    )).toDF("Val1", "Val2")



    val colMap=Map("Val1"->"Val2")
    /*val sofaValueColumns=Array("CBD_Cardio_SOFA","CBD_Coag_SOFA",
      "CBD_Dobut_SOFA","CBD_Hepat_SOFA",
      "CBD_Neuro_SOFA","CBD_Renal_SOFA",
      "CBD_Resp_SOFA","CBD_SOFA_NA","CBD_SOFA")
    val colMap=Map[String,String]()
    for(colName<-sofaValueColumns){
      colMap(colName+"_D01")=(colName+"_D01-D02")
    }

    println(colMap.toString())*/

    CompareRowValueOfTwoCols(df,colMap)
  }

  /** This function takes a data frame and a map of (colNum->colName), the elements of the return map are
    * sorted by the column number with asc order.
    *
    * @author Pengfei liu
    * @version 1.0
    * @since 2018-12-20
    * @param df The source data frame.
    * @return a Map[Int, String]*/
  def CompareRowValueOfTwoCols(df:DataFrame, colMap: Map[String,String]): Unit ={
    for ((k,v)<-colMap){
      val res1=df.filter(!(df(k)===df(v))).select(k,v)
       println(s"List of possible colision value row with null")
      res1.show(10,false)

      val res2=res1.filter((!(res1(k)==="null"))&&(!(res1(v)==="null"))).select(k,v)
      println(s"List of possible colision value row without null")
      res2.show()
    }
  }
}
