package org.pengfei.Lesson10_Spark_Application_ETL

import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.first
object Pivot {

  def pivotSummary(df:DataFrame):DataFrame={

    import df.sparkSession.implicits._
    val schema= df.schema
    val ls:Dataset[(String,String,Double)]=df.flatMap(row=>{
      val metric=row.getString(0)
      (1 until row.length).map(i=>{
        (metric,schema(i).name,row.getString(i).toDouble)
      })
    })
    val lf=ls.toDF("metric","field","value")
    //lf.show(5)
   val wf= lf.groupBy("field").pivot("metric",Seq("count","mean","stddev","min","max")).agg(first("value"))
   // wf.show(5)
    return wf
  }

}
