package org.pengfei.spark.application.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object IoTDeviceGeoIPDS {
def main(args:Array[String])={
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val spark = SparkSession.builder().
    master("local").
    appName("IoTDeviceGeoIPDS").
    getOrCreate()

  import spark.implicits._

  val dataSetFilePath="file:///DATA/data_set/spark/iot_devices.json"

  case class DeviceIoTData (battery_level: Long, c02_level: Long, cca2: String,
                             cca3: String, cn: String,
                             device_id: Long,
                             device_name: String,
                             humidity: Long,
                             ip: String,
                             latitude: Double,
                             longitude: Double,
                             scale: String,
                             temp: Long,
                             timestamp: Long
                           )

  val ds=spark.read.json(dataSetFilePath)
  /*println(ds.getClass().getName)
  ds.show(5)

  */
  //ds.printSchema()

  //Q1.
  // filter out all devices whose temperature exceed 25 degrees and generate
  // another Dataset with three fields that of interest and then display
  // the mapped Dataset
  val dsTemp25=ds.filter(ds("temp")>25).select(ds("temp"),ds("device_name"),ds("cca3"))
  //dsTemp25.show(5)

  //Q2.
  // Apply higher-level Dataset API methods such as groupBy() and avg().
  // Filter temperatures > 25, along with their corresponding
  // devices' humidity, compute averages, groupBy cca3 country codes,
  // and display the results, using table and bar charts
  val dsAvgTmp = ds.filter(ds("temp")>25).select(ds("temp"),ds("humidity"),ds("cca3")).groupBy(ds("cca3")).avg()
  //dsAvgTmp.show(5)

 //Q3.
  // Select individual fields using the Dataset method select()
  // where battery_level is greater than 6. Note this high-level
  // domain specific language API reads like a SQL query
  val dsBatteri6=ds.select($"battery_level", $"c02_level", $"device_name").where($"battery_level" > 6).sort($"c02_level")
 // dsBatteri6.show(5)

  //Q4.
  // Use spark sql query
  ds.createOrReplaceTempView("iot_device_data")

  val sqlDF = spark.sql("select * from iot_device_data")
  //sqlDF.show(5)

  //Q5
  // count all devices for all countries
  val deviceCountDF = spark.sql("select cca3, count(distinct device_id) as count_device from iot_device_data group by cca3 order by count_device desc limit 100")
  //deviceCountDF.show(5)

  //Q6
  // Select all countries' devices with high-levels of C02 and group by cca3 and order by device_ids
  val redDeviceCountDF = spark.sql("select cca3, count(distinct device_id) as count_device from iot_device_data where lcd == 'red' group by cca3 order by count_device desc limit 100 ")
  val red=ds.filter(ds("lcd")==="red").select(ds("cca3"),ds("device_id")).groupBy(ds("cca3")).count().orderBy($"count".desc)
  //red.show(5)
  //redDeviceCountDF.show(5)

  //Q7
  //find out all devices in countries whose batteries need replacements
  val batteryRepDF= spark.sql("select count(distinct device_id) as count_device, cca3 from iot_device_data where battery_level == 0 group by cca3 order by count_device desc limit 100 ")
  val battery=ds.filter(ds("battery_level")===0).select(ds("cca3"),ds("device_id")).groupBy(ds("cca3")).count().orderBy($"count".desc)
  //battery.show(5)
  //batteryRepDF.show(5)

  //Q8 convert data to RDD
  //val deviceEventsDS = ds.select($"device_name",$"cca3", $"c02_level").where($"c02_level" > 1300)
  val deviceEventsDS = ds.filter($"c02_level">1300).select($"device_name",$"cca3", $"c02_level")
  val eventsRDD = deviceEventsDS.rdd.take(10)
  //eventsRDD.foreach(println)

  //Q9 get all devices in china
  val chineseDevices = ds.filter($"cca3"==="CHN").orderBy($"c02_level".desc)
  chineseDevices.show(5)
}

/*
*Q6
* +----+-----+
|cca3|count|
+----+-----+
| USA|17489|
| CHN| 3616|
| KOR| 2942|
| JPN| 2935|
| DEU| 1966|
+----+-----+

Q7
+------------+----+
|count_device|cca3|
+------------+----+
|        7043| USA|
|        1415| CHN|
|        1217| KOR|
|        1210| JPN|
|         760| DEU|
+------------+----+
* */

}
