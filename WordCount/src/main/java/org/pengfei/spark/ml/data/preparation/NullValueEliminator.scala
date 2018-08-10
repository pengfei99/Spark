package org.pengfei.spark.ml.data.preparation

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

class NullValueEliminator {
def main(args:Array[String]): Unit ={
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val spark = SparkSession.builder().
    master("local[2]"). //spark://10.70.3.48:7077 remote
    appName("CalHousing").
    getOrCreate()
  //spark.conf.set("")
  import spark.implicits._

  case class Company(cName: String, cId: String, details: String)
  case class Employee(name: String, id: String, email: String, company: Company)

  val e1 = Employee("n1", null, "n1@c1.com", Company("c1", "1", "d1"))
  val e2 = Employee("n2", "2", "n2@c1.com", Company("c1", "1", "d1"))
  val e3 = Employee("n3", "3", "n3@c1.com", Company("c1", "1", "d1"))
  val e4 = Employee("n4", "4", "n4@c2.com", Company("c2", "2", "d2"))
  val e5 = Employee("n5", null, "n5@c2.com", Company("c2", "2", "d2"))
  val e6 = Employee("n6", "6", "n6@c2.com", Company("c2", "2", "d2"))
  val e7 = Employee("n7", "7", "n7@c3.com", Company("c3", "3", "d3"))
  val e8 = Employee("n8", "8", "n8@c3.com", Company("c3", "3", "d3"))
  val employees = Seq(e1, e2, e3, e4, e5, e6, e7, e8)
  print(employees.getClass().getName())

  val sc=spark.sparkContext
  val rdd=sc.parallelize(employees)
  print(rdd.getClass().getName())
  //val df=spark.createDataFrame(rdd)
  //val df=sc.parallelize(employees).toDF
//df.show()
  //df.filter("id is null").show()
 // df.withColumn("id", when($"id".isNull, 0).otherwise(1)).show



}

  def removeNullValueOfFeatureColumns(dataFrame:DataFrame,columnNames:Array[String]):DataFrame={
   var result:DataFrame=dataFrame
    for(columnName <- columnNames){
    result=result.filter(result(columnName).isNotNull)

  }
    return result
  }
}
