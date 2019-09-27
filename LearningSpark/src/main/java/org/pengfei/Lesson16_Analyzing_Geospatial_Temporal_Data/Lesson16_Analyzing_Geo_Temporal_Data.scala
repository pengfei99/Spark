package org.pengfei.Lesson16_Analyzing_Geospatial_Temporal_Data

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import spray.json.{JsObject, JsString, JsValue, RootJsonFormat}

object Lesson16_Analyzing_Geo_Temporal_Data {

  /**********************************************************************************************************
    * *************************************16.1 Introduction ********************************************
    * **************************************************************************************************/

  /* In this lesson, we will use new york taxi trip data to illustrate how to work with temporal data
  * (dates and times), and geospatial information (points of longitude, and latitude). The data can be download from
  * http://www.andresmh.com/nyctaxitrips. The trip_data.7z requires 3.8GB of space. /
  *
  * Each row of the file after header represents a single taxi ride in CSV format. We have some attributes of the
  * cab (a hashed version of the medallion number) as well as the driver (a hashed version of the hack license,
  * which is what licenses to drive taxis are called). some temporal information about when the trip started and ended,
  * and the longitude/latitude coordinates for where teh passengers were picked up and dropped off.*/

  /**********************************************************************************************************
    * *************************************16.2 Working with Geospatial and Temporal data *********************
    * **************************************************************************************************/

  /* Temporal data treatement become easier in java 8 thanks to the java.time package, whose design is based on the
  * highly successful JodaTime library.
  *
  * Geospatial data analyze isn't nearly so simple, there are many different libraries and tools that have different
  * functions, states of development, and maturity levels, so there is not a dominant Java library for all geospatial
  * use cases.
  *
  * The first thing you must consider when choosing a library is determine what kind of geospatial data you will work
  * with. There are two major kinds:
  * - Vector
  * - Raster
  *
  * There are different tools for working with each type. In our case, we have latitude and longitude for our taxi
  * trip records, and vector data stored in the Geo-JSON format that represents the boundaries of the different boroughs
  * of new york. So we need a library that can parse GeoJSON data and can handle spatial relationshipsf, like detecting
  * wheter a given longitude/latitude pair is contianed inside a polygon that represents the boundaries of a particular
  * borough.
  *
  * We use the Esri Geometry API for Java, which has few dependencies and can analyze spatial relationships but can
  * only parse a subset of the GeoJSON standard, so it won’t be able to parse the GeoJSON data we downloaded without
  * us doing some preliminary data munging.
  *
  * For a data analyst, this lack of tooling might be an insurmountable problem. But we are data scientists: if our
  * tools don’t allow us to solve a problem, we build new tools. In this case, we will add Scala functionality for
  * parsing all of the GeoJSON data, including the bits that aren’t handled by the Esri Geometry API, by leveraging
  * one of the many Scala projects that support parsing JSON data. The original code of the book can be found
  * https://github.com/sryza/aas/tree/master/ch08-geotime . The code of geo-json treatment has also been made available
  * as a standalone library on GitHub (https://github.com/jwills/geojson), where it can be used for any kind of
  * geospatial analysis project in Scala.
  *
  *
  * */

  /* ***************************** 16.2.1 Exploring the Esri Geometry API  ***************************************/

  /* The core data type of the Esri librarty is the Geometry object. A Geometry describes a shape, accompanied by a
  * geolocation where that shape resides. The library contains a set of spatial operations by a geolocation where
  * that shape resides. The library contains a set of spatial operations that allows analyzing geometries and their
  * relationships. These operations can do things like tell us the area of a geometry and whether two geometries
  * overlap, or compute the geometry formed by the union of two geometries.
  *
  * In our case, we'll have Geometry objects representing dropoff points for cab rides, and Geometry objects that
  * represent the boundaries of a borough in NYC. The spatial relationship we're interested in is containment: is
  * a given point in space located inside one of the polygons associated with a borough of Manhattan?
  *
  * The Esri API provides a convenience class called GeometryEngine that contains static methods for performing all
  * of the spatial relationship operation, including a contains operation. The contains method takes three arguments
  * two Geometry objects, and one instance of the SpatialReference class, which represents the coordinate system used
  * to perform the geospatial calculations.
  *
  * For maximum precision, we need to analyze spatial relationships relative to a coordinate plane that maps each
  * point on the misshapen spheroid that is planet Earth into a 2D coordinate system. Geospatial engineers have a
  * standard set of well-known identifiers (referred to as WKIDs) that can be used to reference the most commonly
  * used coordinate systems. For our purposes, we will be using WKID 4326, which is the standard coordinate
  * system used by GPS.
  * */

  /* We create wrapper classes such as RichGeometry classes that encapsule Esri Geometry object with some useful helper
  * method, we also declare a companion object for RichGeometry that provide support for implicitly converting instances
   * of the Geometry class into RichGeometry instances.
   * To be able to take advantage of this conversion, we need to import the implicit function definition into the Scala
   * environment, like import RichGeometry._
   *
   * The full code is under RichGeometry.scala*/

  /*************************************** 16.2.2 Intro to GeoJSON API  ******************************************/

  /* The data we'll use for the boundaries of boroughs in New York city comes written in a format called GeoJSON.
  * The core object in the json file is called a feature, which is made up of a geometry instance and a set of
  * key-value pairs called properties. A geometry is a shape like a point, line, or polygon. A set of features is
  * called a FeatureCollection. Let's pull down the GeoJSON data for the NYC borough maps and take a look at its
  * structure.
  *
  * You can download data from https://nycdatastables.s3.amazonaws.com/2013-08-19T18:15:35.172Z/nyc-borough-boundaries-polygon.geojson
  * Rename the download file to nyc-boroughs.geojson
  *
  * head of the file looks like this
  *
  * { "type": "FeatureCollection",
  * "features": [ { "type": "Feature", "id": 0, "properties": { "boroughCode": 5, "borough": "Staten Island",
  * "@id": "http:\/\/nyc.pediacities.com\/Resource\/Borough\/Staten_Island" }, "geometry": { "type": "Polygon",
  * "coordinates": [ [ [ -74.050508064032471, 40.566422034160816 ], [ -74.049983525625748, 40.566395924928273 ],
  * [ -74.049316403620878, 40.565887747780437 ], [ -74.049236298420453, 40.565362736368101 ], [ -74.050026201586434,
  * 40.565318180621134 ], [ -74.050906017050892, 40.566094342130597 ], [ -74.050679167486138, 40.566310845736403 ],
  * [ -74.05107159803778, 40.566722493397798 ], [ -74.050508064032471, 40.566422034160816 ] ] ] } }
  *
  * We can find the name of borough "staten Island" and other related information.
  *
  * */

  /* The Esri Geometry API will help us parse the Geometry JSON inside each feature but won't help us parse the id or
  * the properties fields, which can be arbitrary JSON objects. To parse these, we need to use a Scala JSON lib.
  *
  * We choose Spray, an open source toolkit for building web services with Scala, provides a JSON library that is up
  * to the task. Spray-json allows us to convert any Scala object to a corresponding JsValue by calling an implicit
  * toJson method. It also allows us to convert any String that contains JSON to a passed intermediate form by calling
  * parseJson, and then converting it to a Scala type T by calling convertTo[T] on the intermediate type. Spray comes
  * with built-in conversion implementations for the common Scala primitive types as well as tuples and collection types
  * and it also has a formatting library that allows us to declare the rules for converting custom types like our
  * RichGeometry class to and from JSON.*/

// The code of case class such as Feature, FeatureCollection can be found in GeoJson

  /* After we have defined the case classes for representing the GeoJSON data, we need to define the formats that tell
  * Spray how to convert between our domain objects (RichGeometry, Feature, and FeatureCollection) and a corresponding
  * JsValue instance. To do this we need to create Scala singleton objects that extend the RootJsonFormat[T] trait,
  * which defines abstract read(jsv: JsValue): T and write(t:T):JsValue
  *
  * For the RichGeometry class, we can delegate most of the parsing and formatting logic to the Esri Geometry API,
  * particularly the geometryToGeoJson and geometryFromGeoJson methods on the GeometryEngine class. But for our case
  * classes, we need to write the formatting code ourselves. Here’s the formatting code for the Feature case class,
  * including some special logic to handle the optional id field
  * Page 176*/

  /**********************************************************************************************************
    * *************************************16.4 Preparing Taxi trip data ******************************
    * **************************************************************************************************/

  def main(args:Array[String]):Unit={
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val spark=SparkSession.builder().appName("Lesson16_Analyzing_Geo_Temporal_Data").master("local[2]").getOrCreate()
    import spark.implicits._
// read config file
    val sparkConfig = ConfigFactory.load("application.conf").getConfig("spark")
    val  path= sparkConfig.getString("sourceDataPath")

    val filePath=s"${path}/spark_lessons/Lesson16_Analyzing_Geo_Temporal_Data/trip_data_sample.csv"

    val taxiRaw=spark.read.option("header","true").csv(filePath)

    taxiRaw.show(1)
  }

  case class Trip(
                   license: String,
                   pickupTime: Long,
                   dropoffTime: Long,
                   pickupX: Double,
                   pickupY: Double,
                   dropoffX: Double,
                   dropoffY: Double)

}
