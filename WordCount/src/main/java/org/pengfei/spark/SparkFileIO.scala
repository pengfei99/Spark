package org.pengfei.spark

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.json.JSON

object SparkFileIO {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkFileIO").setMaster("local")
    val sc = new SparkContext(conf)

    val inputFile= "file:///tmp/test.json"

    val textFile= sc.textFile(inputFile)
    val result= textFile.map(s=>JSON.parseFull(s))

    result.foreach( {r => r match {
      case Some(map: Map[String, Any]) => println(map)
      case None => println("Parsing failed")
      case other => println("Unknown data structure: " + other)
    }
    }
    )
  }



}
