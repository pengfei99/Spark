package org.pengfei.Lesson04_Spark_SQL

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{ArrayType, DataType, DataTypes, IntegerType, MapType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.{col, from_json, struct, when}
import scala.io.Source


/** ******************************************************************************************************
 * ********************************* 07 Define schema for data frames ************************************
 * ******************************************************************************************************/

/** In this lesson, we will use Spark SQL "StructType & StructField" classes to programmatically specify the
 * schema of a DataFrame and creating complex columns like nested struct, array and map columns.
 *
 * - StructType(spark.sql.types.StructType) is a collection of StructFields
 * - StructField(spark.sql.types.StructField) defines:
 *       - column name(String): the name of
 *       - column data type(DataType):
 *       - nullable(boolean): A boolean value to indicate if the field can be nullable or not
 *       - metadata(MetaData):
 *
 * The schema is used when we read semi-structured data such as csv.
 * */

object Lesson04_7_Spark_SQL_Schema {
  def main(args: Array[String]) = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    /*In this exc01, I will use a yelp data set to illustrate how to do data analytics with spark*/
    val sparkConfig = ConfigFactory.load("application.conf").getConfig("spark")
    val path = sparkConfig.getString("sourceDataPath")
    val filePath = s"${path}/spark_lessons/Lesson04_Spark_SQL/yelp_academic_dataset_business.json"

    val spark = SparkSession.builder().master("local[2]").appName("Lesson4_7_Spark_SQL_Schema").getOrCreate()


    /** ***********************************************************************************************
     * *************************** 7.1. Data types ***********************************************
     * ************************************************************************************************/

    /** 7.1.1 Spark Sql data types
     * Spark SQL supports the following data types:
     * StringType
     * HiveStringType
     * ArrayType
     * MapType
     * StructType
     * DateType
     * TimestampType
     * BooleanType
     * CalendarIntervalType
     * ObjectType
     * BinaryType, ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType, DecimalType, NumericType
     * NullType
     *
     * All the numeric type such as Integer, Long, are the subclass of NumericType.
     * All above type class are the subclass of the DataType class. It provides a list of functions to show the
     * properties of each type instances.
     * */


    /** 7.1.2
     * You can noticed in exp1, exp2, a type can be exported in json format. This json string can be used to
     * create a new data type class.
     * */

    //dataTypeExample(spark)

    /** 7.1.3 ArrayType, MapType and StructType
     *
     * We need to pay more attentions to ArrayType, MapType and StructType. They can be very useful in some cases. And
     * they are well supported in parquet format, unlike user defined struct type which may impact performance.
     * */

    /** ***********************************************************************************************
     * *************************** 7.2 Schema ***********************************************
     * ************************************************************************************************/

    /** 7.2.1 StructField
     * StructField is the basic building block of a schema, it defines the name, type, nullable, metadata of each column
     * For example, the StructField("firstName",StringType,true) defines a column named "firstName" of type string, and
     * nullable
     *
     * */


    /** 7.2.2 StructType
     * StructType is an array of StructField. A structType can be considered as the schema of a dataset.
     *
     * In exp5, we use StructType and Field to define a schema. And this schema is used to convert an RDD to a
     * data frame
     * */

    //schemaExample(spark)

    /** 7.2.3 Nested StructType
     * As we mentioned before in section 7.1. SparkSQL supports not only the simple primitive types, but also complex
     * array, map, and nested struct types.
     * In exp6. we define a schema which has array, map, and nested struct types
     * */

    //nestedTypeExample(spark)

    /** ***********************************************************************************************
     * *************************** 7.3 Export and import Schema *****************************************
     * ************************************************************************************************/

    /** 7.3.1 Export schema to json
     * */

    //exportSchemaExample(spark)

    // importSchemaExample(spark)
    // importSchemaFromDDLExample(spark)

    /** ***********************************************************************************************
     * *************************** 7.4 Update schema *****************************************
     * ************************************************************************************************/

    /**
     * The schema of a data frame is updated when we create or delete columns in it. In exp10, we use function withColumn
     * to create new column and drop old column.
     **/
    updateSchemaExample(spark)

    /** ***********************************************************************************************
     * *************************** 7.5 Check metadata of a schema *****************************************
     * ************************************************************************************************/

    /**
     * If you want to perform some checks on metadata of the DataFrame, for example, if a column or field exists
     * in a DataFrame or data type of column; we can easily do this using several functions on SQL StructType
     * and StructField
     *
     * In exp11, we checked the name of column and the type.
     **/
  }

  /** ******************************7.1 Data type example ***************************************/
  def dataTypeExample(spark: SparkSession): Unit = {
    /* Exp1. show the details of String*/
    val strType = DataTypes.StringType
    println("json : " + strType.json)
    println("prettyJson : " + strType.prettyJson)
    println("simpleString : " + strType.simpleString)
    println("sql : " + strType.sql)
    println("typeName : " + strType.typeName)
    println("catalogString : " + strType.catalogString)
    println("defaultSize : " + strType.defaultSize)

    /* Exp2. show the details of Array of integer*/
    val arrType = ArrayType(IntegerType, false)

    println("json : " + arrType.json)
    println("prettyJson : " + arrType.prettyJson)
    println("simpleString : " + arrType.simpleString)
    println("sql : " + arrType.sql)
    println("typeName : " + arrType.typeName)
    println("catalogString : " + arrType.catalogString)
    println("defaultSize : " + arrType.defaultSize)

    /*Exp3. Create array type from json*/
    val arrayFromJson = DataType.fromJson("""{"type":"array","elementType":"string","containsNull":false}""".stripMargin)
    println(arrayFromJson.getClass)

    /*Exp4. Create string type from json*/
    val strFromJson2 = DataType.fromJson("\"string\"")
    println(strFromJson2.getClass)

  }


  /** *****************************7.2 schema example ******************************************/

  def schemaExample(spark: SparkSession): Unit = {
    /*exp5. Convert rdd to df with a given schema*/
    val demoData = Seq(Row("James ", "", "Smith", "36636", "M", 3000),
      Row("Michael ", "Rose", "", "40288", "M", 4000),
      Row("Robert ", "", "Williams", "42114", "M", 4000),
      Row("Maria ", "Anne", "Jones", "39192", "F", 4000),
      Row("Jen", "Mary", "Brown", "23456", "F", -1)
    )

    val simpleSchema = StructType(Array(
      StructField("firstName", StringType, true),
      StructField("middleName", StringType, true),
      StructField("lastName", StringType, true),
      StructField("id", StringType, true),
      StructField("gender", StringType, true),
      StructField("salary", IntegerType, true)
    ))

    val rdd = spark.sparkContext.parallelize(demoData)
    val df = spark.createDataFrame(rdd, simpleSchema)
    df.printSchema()
    df.show()

  }

  def nestedTypeExample(spark: SparkSession): Unit = {
    /*exp6. array, map, nested-struct*/
    val demoData = Seq(
      Row(Row("James ", "", "Smith"), List("Cricket", "Movies"), Map("hair" -> "black", "eye" -> "brown")),
      Row(Row("Michael ", "Rose", ""), List("Tennis"), Map("hair" -> "brown", "eye" -> "black")),
      Row(Row("Robert ", "", "Williams"), List("Cooking", "Football"), Map("hair" -> "red", "eye" -> "gray")),
      Row(Row("Maria ", "Anne", "Jones"), null, Map("hair" -> "blond", "eye" -> "red")),
      Row(Row("Jen", "Mary", "Brown"), List("Blogging"), Map("white" -> "black", "eye" -> "black"))
    )

    val schema = new StructType()
      // the type of column fullName is a nested structType which has three columns
      .add("fullName", new StructType()
        .add("firstName", StringType)
        .add("middleName", StringType)
        .add("lastName", StringType))
      // the type of column hobbies is an array of string
      .add("hobbies", ArrayType(StringType))
      // the type of column properties is a map of (string->string)
      .add("properties", MapType(StringType, StringType))

    val rdd = spark.sparkContext.parallelize(demoData)
    val df = spark.createDataFrame(rdd, schema)
    df.printSchema()
    df.show(false)
  }

  /** *****************************7.3 export schema example ******************************************/
  def exportSchemaExample(spark: SparkSession): Unit = {
    /*exp7. get schema of a data frame, and export it in string or json*/
    val demoData = Seq(Row("James ", "", "Smith", "36636", "M", 3000),
      Row("Michael ", "Rose", "", "40288", "M", 4000),
      Row("Robert ", "", "Williams", "42114", "M", 4000),
      Row("Maria ", "Anne", "Jones", "39192", "F", 4000),
      Row("Jen", "Mary", "Brown", "23456", "F", -1)
    )

    val simpleSchema = StructType(Array(
      StructField("firstName", StringType, true),
      StructField("middleName", StringType, true),
      StructField("lastName", StringType, true),
      StructField("id", StringType, true),
      StructField("gender", StringType, true),
      StructField("salary", IntegerType, true)
    ))

    val rdd = spark.sparkContext.parallelize(demoData)
    val df = spark.createDataFrame(rdd, simpleSchema)

    //get the schema of a data frame. The returned schema is a structureType
    val schema: StructType = df.schema
    println("TreeString : " + schema.treeString)
    println("json : " + schema.json)
    println("prettyJson : " + schema.prettyJson)
    println("simpleString : " + schema.simpleString)
    println("sql : " + schema.sql)
    println("typeName : " + schema.typeName)
    println("catalogString : " + schema.catalogString)
    println("defaultSize : " + schema.defaultSize)

  }

  def importSchemaExample(spark: SparkSession): Unit = {
    /*exp8. get schema from a json file, and apply it on an dataframe*/
    val demoData = Seq(Row("James ", "", "Smith", "36636", "M", 3000),
      Row("Michael ", "Rose", "", "40288", "M", 4000),
      Row("Robert ", "", "Williams", "42114", "M", 4000),
      Row("Maria ", "Anne", "Jones", "39192", "F", 4000),
      Row("Jen", "Mary", "Brown", "23456", "F", -1)
    )
    val jsonString =
      """
        |{
        |  "type" : "struct",
        |  "fields" : [ {
        |    "name" : "firstName",
        |    "type" : "string",
        |    "nullable" : true,
        |    "metadata" : { }
        |  }, {
        |    "name" : "middleName",
        |    "type" : "string",
        |    "nullable" : true,
        |    "metadata" : { }
        |  }, {
        |    "name" : "lastName",
        |    "type" : "string",
        |    "nullable" : true,
        |    "metadata" : { }
        |  }, {
        |    "name" : "id",
        |    "type" : "string",
        |    "nullable" : true,
        |    "metadata" : { }
        |  }, {
        |    "name" : "gender",
        |    "type" : "string",
        |    "nullable" : true,
        |    "metadata" : { }
        |  }, {
        |    "name" : "salary",
        |    "type" : "integer",
        |    "nullable" : true,
        |    "metadata" : { }
        |  } ]
        |}
        |""".stripMargin
    val schema = DataType.fromJson(jsonString).asInstanceOf[StructType]
    val dfFromJson = spark.createDataFrame(
      spark.sparkContext.parallelize(demoData), schema)
    dfFromJson.printSchema()
    dfFromJson.show(false)


  }

  def importSchemaFromDDLExample(spark: SparkSession): Unit = {
    /*exp9. get schema from a ddl file, and apply it on an dataframe*/
    val demoData = Seq(Row(Row("James ", "", "Smith"), "36636", "M", 3000),
      Row(Row("Michael ", "Rose", ""), "40288", "M", 4000),
      Row(Row("Robert ", "", "Williams"), "42114", "M", 4000),
      Row(Row("Maria ", "Anne", "Jones"), "39192", "F", 4000),
      Row(Row("Jen", "Mary", "Brown"), "23456", "F", -1))
    /*We can also create a schema from ddl file*/
    val ddlSchemaStr = "`fullName` STRUCT<`first`: STRING, `last`: STRING,`middle`: STRING>,`id` STRING,`gender` STRING," +
      "`salary` INT"
    val ddlSchema = StructType.fromDDL(ddlSchemaStr)
    val dfFromDDL = spark.createDataFrame(
      spark.sparkContext.parallelize(demoData), ddlSchema)
    dfFromDDL.printSchema()
    dfFromDDL.show(false)
  }

  def updateSchemaExample(spark: SparkSession): Unit = {

    /*exp10. get schema from a ddl file, and apply it on an dataframe*/
    val demoData = Seq(Row(Row("James ", "", "Smith"), "36636", "M", 3000),
      Row(Row("Michael ", "Rose", ""), "40288", "M", 4000),
      Row(Row("Robert ", "", "Williams"), "42114", "M", 4000),
      Row(Row("Maria ", "Anne", "Jones"), "39192", "F", 4000),
      Row(Row("Jen", "Mary", "Brown"), "23456", "F", -1))
    /*We can also create a schema from ddl file*/
    val ddlSchemaStr = "`fullName` STRUCT<`first`: STRING, `last`: STRING,`middle`: STRING>,`id` STRING,`gender` STRING," +
      "`salary` INT"
    val ddlSchema = StructType.fromDDL(ddlSchemaStr)
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(demoData), ddlSchema)
    df.printSchema()
    df.show(false)

    val updatedDF = df.withColumn("OtherInfo", struct(
      col("id").as("identifier"),
      col("gender").as("gender"),
      col("salary").as("salary"),
      when(col("salary").cast(IntegerType) < 2000, "Low")
        .when(col("salary").cast(IntegerType) < 4000, "Medium")
        .otherwise("High").alias("Salary_Grade")
    )).drop("id", "gender", "salary")

    updatedDF.printSchema()
    updatedDF.show(false)

    /*exp 11*/
    //It should print true, because df has a column named gender
    println(df.schema.fieldNames.contains("gender"))
    //It should print false, because df does not have a column named email
    println(df.schema.fieldNames.contains("email"))
    //It should print true, because df has a column named gender which has type string
    println(df.schema.contains(StructField("gender", StringType, true)))
    //It should print false, because gender column is not integer
    println(df.schema.contains(StructField("gender", IntegerType, true)))
  }
}
