package org.pengfei.tmp

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.pengfei.Lesson04_Spark_SQL.MergeListsUDAF

object Test {

  def main(args:Array[String]):Unit={
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val spark=SparkSession.builder().appName("Test").master("local[2]").getOrCreate()
    import spark.implicits._

   /* val clusterLabels=spark.sparkContext.parallelize(Array((0,"normal"),(0,"normal"),(0,"back"),(1,"neptune"),(1,"satan"),(1,"nep"))).toDF("cluster","label").as[(Int,String)]

    spark.udf.register("labelCount",(label:String)=>labelCount(label))
   val trans=clusterLabels.withColumn("labelWithCount",expr("labelCount(label)"))
    val groupedClusterLabels=trans.groupBy("cluster").agg(collect_list("labelWithCount").as("labels"))

    groupedClusterLabels.show()*/

    /*
    val weightedClusterEntropy=groupedClusterLabels.map{

      rows=>
        /*val labelSize=rows.labels.s
        val labelCounts=labels.*/
        println(s"${rows.mkString(";")}")
    }
    */

    /*
     clusterLabels.show()
     spark.udf.register("combineTwo",(cluster:Int,label:String)=>combineTwoColumn(cluster,label))
     val keyValue=clusterLabels.withColumn("clusterLabels",expr("combineTwo(cluster,label)")).select("clusterLabels")
     keyValue.show()

     val weightedClusterEntropy=clusterLabels.groupByKey{
       case(cluster,_)=> cluster
     }.mapGroups { case (_, clusterLabels) =>

       val labels = clusterLabels.map { case (_, label) => label }.toSeq
       println(s"${labels.toString}")
       val labelCounts = labels.groupBy(identity).values.map(_.size)
       val labelSize=labels.size
       println(s"labelCounts: ${labelCounts}")
       println(s"labelSize : ${labelSize}")
       labelSize * entropy(labelCounts)
     }.collect()*/

    /*mapGroups{case (_,clusterLabels)=>
        val labels = clusterLabels.map{case(_, label)=>label}.toSeq
    }*/
    //println(s"${weightedClusterEntropy.toArray.mkString(";")}")


  }




  def myConcat(text1:String,text2:String):String={
    text1.concat(text2)
  }

  def combineTwoColumn(cluster:Int,label:String):(Int,String)={
    return (cluster,label)
  }

  def labelCount(label:String):(String,Int)={
    return (label,1)
  }

  def entropy(counts: Iterable[Int]):Double={
    // get all positive values in the counts collection
    val values=counts.filter(_ >0)
    // cast all values to double and do sum
    val n = values.map(_.toDouble).sum
    val entropy=values.map{v=>
      //calculate p first
      val p=v / n
      //
      -p * math.log(p)
    }.sum

    /* We can use the following code, if you don't want to define a local variable

    values.map { v =>
      -(v / n) * math.log(v / n)
    }.sum
    */

    return entropy
  }
}
