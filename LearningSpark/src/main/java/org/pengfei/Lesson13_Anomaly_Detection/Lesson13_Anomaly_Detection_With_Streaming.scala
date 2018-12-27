package org.pengfei.Lesson13_Anomaly_Detection

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{Dataset, SparkSession}

object Lesson13_Anomaly_Detection_With_Streaming {

  def main(args:Array[String]):Unit={
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val spark=SparkSession.builder.appName("Lesson13_Anomaly_Detection").master("local[2]").getOrCreate()
    import spark.implicits._

    val host="localhost"
    val port=9999
    val lines=spark.readStream.format("socket").option("host",host).option("port",port).load()

    val data=lines.as[String].flatMap(_.split(","))
    data.isStreaming    // Returns True for DataFrames that have streaming sources
    data.printSchema
    val countDs = data.groupByKey(value=>value).flatMapGroups{
      case (value, iter)=> Iterator((value, iter.length))
    }.toDF("value", "count")
    val query=data.writeStream.outputMode("append").format("console").trigger(Trigger.ProcessingTime("5 seconds")).start()


    query.awaitTermination()
  }

  /* 0,tcp,http,SF,215,45076,0,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,1,1,0.00,0.00,0.00,0.00,1.00,0.00,0.00,0,0,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,normal.

*/
  case class Connection(duration:Int, protocol_type:String, service:String, flag:String,
  src_bytes:Int, dst_bytes:Int, land:Int, wrong_fragment:Int, urgent:Int,
  hot:Int, num_failed_logins:Int, logged_in:Int, num_compromised:Int,
  root_shell:Int, su_attempted:Int, num_root:Int, num_file_creations:Int,
  num_shells:Int, num_access_files:Int, num_outbound_cmds:Int,
  is_host_login:Int, is_guest_login:Int, count:Int, srv_count:Int,
  serror_rate:Double, srv_serror_rate:Double, rerror_rate:Double, srv_rerror_rate:Double,
  same_srv_rate:Double, diff_srv_rate:Double, srv_diff_host_rate:Double,
  dst_host_count:Int, dst_host_srv_count:Int,
  dst_host_same_srv_rate:Double, dst_host_diff_srv_rate:Double,
  dst_host_same_src_port_rate:Double, dst_host_srv_diff_host_rate:Double,
  dst_host_serror_rate:Double, dst_host_srv_serror_rate:Double,
  dst_host_rerror_rate:Double, dst_host_srv_rerror_rate:Double,
  label:String)



  /*
  *
  * case Array(duration, protocol_type, service, flag,
      src_bytes, dst_bytes, land, wrong_fragment, urgent,
      hot, num_failed_logins, logged_in, num_compromised,
      root_shell, su_attempted, num_root, num_file_creations,
      num_shells, num_access_files, num_outbound_cmds,
      is_host_login, is_guest_login, count, srv_count,
      serror_rate, srv_serror_rate, rerror_rate, srv_rerror_rate,
      same_srv_rate, diff_srv_rate, srv_diff_host_rate,
      dst_host_count, dst_host_srv_count,
      dst_host_same_srv_rate, dst_host_diff_srv_rate,
      dst_host_same_src_port_rate, dst_host_srv_diff_host_rate,
      dst_host_serror_rate, dst_host_srv_serror_rate,
      dst_host_rerror_rate, dst_host_srv_rerror_rate,
      label)=Connection(duration.toInt, protocol_type, service, flag,
    src_bytes, dst_bytes, land, wrong_fragment, urgent,
    hot, num_failed_logins, logged_in, num_compromised,
    root_shell, su_attempted, num_root, num_file_creations,
    num_shells, num_access_files, num_outbound_cmds,
    is_host_login, is_guest_login, count, srv_count,
    serror_rate, srv_serror_rate, rerror_rate, srv_rerror_rate,
    same_srv_rate, diff_srv_rate, srv_diff_host_rate,
    dst_host_count, dst_host_srv_count,
    dst_host_same_srv_rate, dst_host_diff_srv_rate,
    dst_host_same_src_port_rate, dst_host_srv_diff_host_rate,
    dst_host_serror_rate, dst_host_srv_serror_rate,
    dst_host_rerror_rate, dst_host_srv_rerror_rate,
    label)*/

}
