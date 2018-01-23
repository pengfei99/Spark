package org.pengfei.spark

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkConf, SparkContext}

object SparkHBaseIO {
def main(args:Array[String]): Unit ={
  // create hbase configuration
  val hbaseConf = HBaseConfiguration.create()
  val sparkConf = new SparkConf().setAppName("SparkHBaseIO").setMaster("local")
  val sc = new SparkContext(sparkConf)

  insertRDDtoTable("student",sc)
  getTableAsRDD("student",sc,hbaseConf)

}

  def getTableAsRDD(tableName:String,sparkContext: SparkContext,hbaseConf:Configuration): Unit ={
    //set table name as student
    hbaseConf.set(TableInputFormat.INPUT_TABLE,tableName)
    val stuRDD = sparkContext.newAPIHadoopRDD(hbaseConf,classOf[TableInputFormat],classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    val count = stuRDD.count()
    println("Studnets RDD count: "+ count)
    stuRDD.cache()

    stuRDD.foreach({ case (_,result) =>
      val key = Bytes.toString(result.getRow)
      val name = Bytes.toString(result.getValue("info".getBytes,"name".getBytes))
      val gender = Bytes.toString(result.getValue("info".getBytes,"gender".getBytes))
      val age = Bytes.toString(result.getValue("info".getBytes,"age".getBytes))
      println("Row key:"+key+" Name:"+name+" Gender:"+gender+" Age:"+age)
    })
  }

  def insertRDDtoTable(tableName:String,sc:SparkContext): Unit ={
    sc.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE, tableName)

    val job = Job.getInstance(sc.hadoopConfiguration)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
    // create a rdd with two lines
    val indataRDD = sc.makeRDD(Array("3,Rongcheng,M,26","4,Guanhua,M,32"))
    val rdd = indataRDD.map(_.split(',')).map{arr=>{
      // add row key
      val put = new Put(Bytes.toBytes(arr(0)))
      //add column name, gender, age to column family info
      put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("name"),Bytes.toBytes(arr(1)))
      put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("gender"),Bytes.toBytes(arr(2)))
      put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("age"),Bytes.toBytes(arr(3)))
      (new ImmutableBytesWritable, put)
    }}
    rdd.saveAsNewAPIHadoopDataset(job.getConfiguration())
  }
}
