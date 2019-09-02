package org.pengfei.Lesson04_Spark_SQL

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object Lesson04_1_Spark_SQL_Intro {

  def main(args: Array[String]):Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val spark = SparkSession.builder().master("local[2]").appName("Lesson4_1_Saprk_SQL_Intro").getOrCreate()


    /** ***********************************************************************************************
      * ***********************************4.1 Spark sql introduction ********************************
      * **********************************************************************************************/

    /* Spark SQL is a Spark library that runs on top of Spark. It provides a higher-level abstraction than the Spark
     * core API for processing structured data. Structured data includes data stored in a database, NoSQL data
     * store, Parquet, ORC, Avro, JSON, CSV, or any other (semi-structured) format
     *
     * Spark SQL can be used as a library for developing data processing applications in Scala, Java, Python,
     * or R. It supports multiple query languages, including SQL, HiveQL, and language integrated queries. In
     * addition, it can be used for interactive analytics with just SQL/HiveQL. In both cases, it internally uses the
     * Spark core API to execute queries on a Spark cluster*/

    /** ******************************4.1.1 Integration With other libraries ********************************/
    /* Spark SQL seamlessly integrates with other Spark libraries such as Spark Streaming, Spark ML, and GraphX
    * It can be used for not only interactive and batch processing of historical data but also live data stream
    * processing (not really, but micro batch processing) along with Spark Streaming. Similarly, it can be used
    * in machine learning applications with MLlib and Spark ML. For example, Spark SQL can be used for feature
    * engineering in a machine learning application.*/

    /** ********************************4.1.2 Data Sources *************************************************/

    /* Spark SQL supports a variety of data sources. It can be used to process data stored in a file, a NoSQL
     * datastore, or a database. A file can be on HDFS, S3, or local file system. The file formats supported by Spark
     * SQL include CSV, JSON, Parquet, ORC, and Avro.
     *
     * Spark SQL supports a number of relational databases and NoSQL datastores. The relational databases
     * supported by Spark SQL include PostgreSQL, MySQL, H2, Oracle, DB2, MS SQL Server, and other databases
     * that provide JDBC connectivity. The NoSQL data stores that can be used with Spark SQL include HBase,
     * Cassandra, Elasticsearch, Druid, and other NoSQL data stores. The list of data sources that can be used with
     * Spark SQL keeps growing.*/

    /** ********************************4.1.3 Data Processing interface *************************************************/

    /* Spark SQL exposes three data processing interfaces: SQL, HiveQL and language integrated queries. It translates
     * queries written using any of these interfaces into Spark core API calls.
     *
     * As previously mentioned, both SQL and HiveQL are higher-level declarative languages. In a declarative
     * language, you just specify what you want. They are much easier to learn and use. Therefore, they are popular
     * as the language of choice for data processing and analytics.
     *
     * However, not all programmers know SQL or HiveQL. Instead of forcing these programmers to learn
     * another language, Spark SQL supports language integrated queries in Scala, Java, Python and R. With
     * language integrated queries, Spark SQL adds data processing capabilities to the host language; programmers
     * an process and analyze data using the native host language syntax.
     *
     * In addition, language integrated queries eliminate the impedance mismatch between SQL and the
     * other programming languages supported by Spark. It allows a programmer to query data using SQL and
     * process the results in Scala, Java, Python or R.
     *
     * Another benefit of language integrated queries is that it reduces errors. When SQL is used for querying
     * data, a query is specified as a string. A compiler cannot detect errors within a string. Therefore, errors within
     * a query string are not found until an exception is thrown at runtime. Some of these errors can be eliminated
     * by using equivalent language integrated queries.
     * */

    /** ********************************4.1.4 Hive Interoperability *************************************************/
    /* Spark SQL is compatible with Hive. It not only supports HiveQL, but can also access Hive metastore,
     * SerDes, and UDFs. Therefore, if you have an existing Hive deployment, you can use Spark SQL alongside Hive.
     * You do not need to move data or make any changes to your existing Hive metastore
     *
     * You can also replace Hive with Spark SQL to get better performance. Since Spark SQL supports HiveQL
     * and Hive metastore, existing Hive workloads can be easily migrated to Spark SQL. HiveQL queries run much
     * faster on Spark SQL than on Hive.
     *
     * Starting with version 1.4.0, Spark SQL supports multiple versions of Hive. It can be configured to read
     * Hive metastores created with different versions of Hive.
     *
     * Note that Hive is not required to use Spark SQL. You can use Spark SQL with or without Hive. It has a
     * built-in HiveQL parser. In addition, if you do not have an existing Hive metastore, Spark SQL creates one.
     *
     * */


    /** ***********************************************************************************************
      * ***********************************4.2 Spark Performance ********************************
      * **********************************************************************************************/

    /* Spark sql makes data processing applications run faster using a combinaiton of techniques, including
    * - Reduced disk I/O
    * - In memory columnar caching
    * - Query optimization
    * - Code generation
    * */

    /** **********************************4.2.1 Reduced Disk I/O ***************************************/

    /* Disk I/O is slow. It can be a significant contributor to query execution time. Therefore, Spark SQL reduces
     * disk I/O wherever possible. For example, depending on the data source, it can skip non-required partitions,
     * rows, or columns while reading data.
     *
     * Reading all data to analyze a part of it is inefficient. For example, a query may have filtering clause
     * that eliminates a significant amount of data before further processing. Thus, a lot of I/O is wasted on
     * data that is never used. It can be avoid by partitioning a dataset.
     *
     * Partitioning is a proven technique for improving read performance. A partitioned dataset is split into horizontal
     * slices. Data may be partitioned by one or more columns. With partitioned datasets, spark sql skips the partitions
     * that an application never uses.
     *
     * Predicate Pushdown
     * Spark SQL also reduces disk I/O by using predicate pushdowns if a data source supports it. For example,
     * if you read data from a relational database using Spark SQL and then apply some filtering operation to it,
     * Spark SQL will push the filtering operation to the database. Instead of reading an entire table and then
     * executing a filtering operation, Spark SQL will ask the database to natively execute the filtering operation.
     * Since databases generally index data, native filtering is much faster than filtering at the application layer.*/

    /** ***********************************4.2.2 In Memory Columnar Caching *********************************/

    /* What is a columnar storage?
     * A structured dataset has a tabular format. It is organized into rows and columns.
     * A dataset may have a large number of columns. However, an analytics application generally processes
     * only a small percentage of the columns in a dataset. Nevertheless, if data is stored in a row-oriented
     * storage format, all columns have to be read from disk. Reading all columns is wasteful and slows down an
     * application. Spark SQL supports columnar storage formats such as Parquet, which allow reading of only the
     * columns that are used in a query.
     *
     * Spark SQL allows an application to cache data in an in-memory columnar format from any data source.
     * For example, you can use Spark SQL to cache a CSV or Avro file in memory in a columnar format.
     * When an application caches data in memory using Spark SQL, it caches only the required columns.
     * In addition, Spark SQL compresses the cached columns to minimize memory usage and JVM garbage
     * collection pressure. Use of columnar format for caching allows Spark SQL to apply efficient compression
     * techniques such as run length encoding, delta encoding, and dictionary encoding.
     *
     * Skip Rows
     * If a data source maintains statistical information about a dataset, Spark SQL takes advantage of it. For example,
     * serialization formats such as Parquet and ORC store min and max values for each column in a row group or
     * chunk of rows. Using this information, Spark SQL can skip reading chunk of rows.
     * */

    /** ********************************* 4.2.3 Query Optimization *******************************************/

    /* Similar to database systems, Spark SQL optimizes a query before executing it. It generates an optimized
     * physical query plan when a query is given to it for execution. It comes with a query optimizer called Catalyst,
     * which supports both rule and cost based optimizations. It can even optimize across functions.
     *
     * Spark SQL optimizes both SQL/HiveQL and language integrated queries submitted through its
     * DataFrame(DataSet and dataframe are unified in java and scala since spark2.0+) API. They share the same
     * query optimizer and execution pipeline. Thus, from a performance perspective it does not matter
     * whether you use SQL, HiveQL or DataFrame API; they go through the same optimization steps. The DataFrame
     * API is covered later in this chapter.
     *
     * Catalyst splits query execution into four phases: analysis, logical optimization, physical planning, and
     * code generation:
     *
     * - The analysis phase starts with an unresolved logical plan and outputs a logical plan.
     *   An unresolved logical plan contains unresolved attributes. An unresolved attribute, for
     *   example, could be a column whose data type or source table is not yet known. Spark
     *   SQL uses rules and a catalog to resolve unbound attributes in a query expression.
     *   The Spark SQL catalog object tracks the columns and tables in all data sources.
     *
     * - In the logical optimization phase, Spark SQL applies rule-based optimizations to
     *   the logical plan generated by the analysis phase. Rule-based optimizations include
     *   constant folding, predicate pushdown, projection pruning, null propagation,
     *   Boolean expression simplification, and other optimizations.
     *
     * - The next phase is the physical planning phase. In this phase, Spark SQL selects an
     *   optimal physical plan using a cost-model. It takes as input the optimized logical
     *   plan generated by the logical optimization phase. Using rules, it then generates
     *   one or more physical plans that can be executed by the Spark execution engine.
     *   Next, it computes their costs and selects an optimal plan for execution. In addition,
     *   it performs rule-based physical optimizations, such as pipelining projections or
     *   filters into one Spark operation. It also pushes operations from the logical plan into
     *   data sources that support predicate or projection pushdown. Spark SQL generates
     *   optimized physical plans even for inefficient logical plans
     *
     * - The last phase is the code generation phase, where Spark SQL compiles parts of a
     *   query directly to Java bytecode. It uses a special Scala language feature to transform a
     *   tree representing a SQL expression to a Scala AST (Abstract Syntax Tree), which is fed
     *   to the Scala compiler at runtime to generate bytecode. Thus, it avoids using the Scala
     *   parser at runtime. This speeds up query execution. The generated code generally
     *   performs as fast as or faster than hand-tuned Scala or Java program.
     * */

    /** ***********************************************************************************************
      * ***********************************4.3 Spark Applications ********************************
      * **********************************************************************************************/

    /** ****************************************4.3.1 ETL (Extract Transform Load) ****************************/

    /* ETL is the process of reading data from one or more sources, applying some transformation on the data, and
     * writing it to another data source. Conceptually, it consists of three steps: extract, transform and load. These
     * steps need not be sequential; an application does not have to extract all the data before moving it to the
     * transform step. Once it has extracted a portion of the data, it may run the three steps in parallel.
     *
     * - Extract : involves reading data from one or more operational systems. The data source could be a database,
     *             API or a file. The source database can be a relational database or a NoSQL data source.
     *             A file can be CSV, JSON, XML, Parquet, ORC, Avro, Protocol Buffers, or any other format.
     *
     * - Transform : involves cleaning and modifying the source data using some rules. For example, rows with invalid
     *               data may be dropped or columns with null values may be populated with some value. It may also
     *               include, concatenating two columns, splitting a column into multiple columns, encoding a column,
     *               translating a column from one encoding to a different encoding, or any other operations which can
     *               make data ready for the destination system
     *
     * - Load : involves writes data to a destination system. The destination can be a database, a file, etc.
     *
     * Generally, ETL is used for data warehousing. Data is collected from a number of different operational systems,
     * cleaned, transformed and stored into a data warehouse. However, ETL is not unique to data warehousing.
     * For example, it can be used to enable sharing of data between two disparate systems. It can be used to convert
     * data from one format to another. Similarly, migrating data from a legacy system to a new system is an
     * ETL process.
     *
     * Spark Sql is great for ETL
     *  - support of many datasource (input and output)
     *  - parallel data transformation for high performance.
     * */

    /** ******************************* 4.3.2 Distributed JDBC/ODBC SQL Query Engine ******************************/

    /* Spark SQL can be used in two ways:
    * - First, it can be used as a library. In this mode, data processing tasks can be expressed as SQL, HiveQL or
    *          language integrated queries within a Scala, Java, Python, or R application.
    * - Second, Spark SQL can be used as a distributed SQL query engine. It comes prepackaged with a
    *           Thrift/JDBC/ODBC server. A client application can connect to this server and submit SQL/HiveQL queries
    *           using Thrift, JDBC, or ODBC interface.
    *
    * Spark SQL provides a command-line client called Beeline, which can be used to submit HiveQL queries, however, the
    * spark JDBC/ODBC server can be queried from any application that supports JDBC/ODBC. For example, you can use it
    * with a sql client such as SQuirrel. Similarly, it can be queried from BI and data visulization tools such as
    * Tableau, Zoomdata and Qlik.
    *
    * The thrift JDBC/ODBC server provides two benefits.
    * - First, it allows query to be written in SQL/HiveQL.
    * - Second, it makes it easy for multiple users to share a single spark cluster.
    *
    * The Spark SQL JDBC/ODBC server looks like a database; however, unlike a database, it does not have a
    * built-in storage engine. It is just a distributed SQL query engine, which uses Spark under the hood and can
    * be used with a variety of a data sources*/

    /** ********************************4.3.3 Data Warehousing ********************************************/

    /* A conventional data warehouse is essentially a database that is used to store and analyze large amounts of
     * data. It consists of three tightly integrated components: data tables, system tables, and a SQL query engine.
      *
      * The data tables, store user data (data source of origin operational systems).
      * The system tables, store metadata about the data in the data tables
      * The SQL query engine provides a SQL interface to store and analyze the data in the data talbes.
      *
      * In general, all the three components are generally packaged together as a proprietary software or appliance
      *
      * Spark SQL can be used to build an open source data warehousing solution. It provide a distributed SQL
      * query engine, which can be paired with a variety of open source storage systems such as HDFS. Thus,
      * it supports a modular architecture that allows users to mix and match different components in the data
      * warehouse stack.
      *
      * For example, user data can be stored in HDFS or S3 using a columnar format such as Parquet or ORC file format.
      * SQL engine can be spark SQL, Hive, Impala, Presto, or Apache Drill.
      *
      * A spark sql based data warehousing solution is more
      * - scalable, (Storage and processing capacity can be easily increased by adding more nodes to hdfs/spark cluster)
      * - economical, (hadoop/spark eco-system are all open sources and run on commodity hardware)
      * - flexible (it supports both schema-on-read and schema-on-write)
      *
      * Schema-on-write systems require a schema to be defined before data can be stored(e.g. traditional databases).
      * These systems require users to create a data model before data can be stored. The benefit of schema-on-write
      * systems is that they provide efficient storage and allow fast interactive queries. Spark SQL support
      * schema-on-write through columnar formats such as Parquet and ORC. A combination of HDFS, columnar file format
      * and sparkSQL can be used to build a high-performance data warehousing solution.
      *
      * Although schema-on-write systems(SOW) enable fast queries, they have a few disadvantages.
      * - Fist, a SOW system requires data to be modeled before it ban be stored.
      * - Second, data ingestion in a SOW system is slow.
      * - Third, schemas are hard to change once a large amount of data has been stored. For example, adding a new
      *   column or changing a column type in a database  with terabytes of data can be chanllenging.
      * - Fouth, SOW can stored unstructured, semi-structrured or multi-structured data.
      *
      *
      * We know that data modeling is not a trivial task. It requires upfront planning and good understanding of data.
      * And it may change with time, We may need to modify the data model as the requirements changes Which SOW can't do
      * well.
      *
      * Schema-on-read (SOR) address preceding issues. It allows data to be stored in its row format. A schema is
      * applied to data when data it is read. Therefore, data can start flowing into a SOR system anytime. A user
      * can store data in its native format without worrying about how it will be queried. At query time, users
      * can apply different schema on the stored data depending on the requirements. SOR ont only enables agility
      * but also alows complex evolving data.
      *
      * The disadvantage of a SOR system is that queries time are slower than SOW. Generally, SOR is used for
      * exploratory analytics, or ETL.
      *
      * Spark SQL can build a data warehouse support both SOW ans SOR. So it can be used for exploratory analytics, etl
      * and high performance analytics.
      *
      *
      * */

    /** ***********************************************************************************************
      * *********************4.4 Spark Application Programming Interface (API) *********************
      * **********************************************************************************************/

    /* Spark SQL provides an application programming interface (API) in multiple languages(e.g. scala, java, python and
     * R at 08-2018.). You can mix SQL/HiveQl and native language API in one spark application
     *
     * Key abstractions:
     * - SQLContext
     * - HiveContext
     * - DataFrame/DataSet*/

    // Create Dataframe for the spark sql api test

    val personsRDD = spark.sparkContext.parallelize(List(
      Person(0, "Alice", 30),
      Person(1, "Bob", 30),
      Person(2, "Charles", 21),
      Person(3, "Defence", 20),
      Person(4, "Eletro", 58),
      Person(5, "Bob", 40)))
    import spark.implicits._
    // the above import is needed to call implicit rdd to dataframe conversion(.toDF)
    // The best way is use CreateDataFrame() method, we will talk in details in next section.
    /* if you see .toDF is not a value of rdd person, it's caused by the case class person is inside the main method,
    * just move it outside the main, it will be ok*/
    val personsDF = personsRDD.toDF()
    personsDF.show()

    /****************************************4.4.1 SQLContext ************************************************/

    /* SQLContext is the main entry point into the Spark SQL library. It is a class defined in the Spark SQL library.
     * A Spark SQL application must create an instance of the SQLContext or HiveContext class.
     *
     * SQLContext is required to create instances of the other classes provided by the Spark SQL library. It is
     * also required to execute SQL queries.
     *
     * In the method sqlContextOperation, we can see how it works*/

   // sqlContextOperation(spark,personsDF)




    /************************************4.4.2 HiveContext(Read/Write hive tables) *******************************/

    /* Since spark2.0+, SparkSession is now the new entry point of Spark that replaces the old SQLContext and
     * HiveContext. In implementation level, SparkSession has merged SQLContext and HiveContext in one object
     * in Spark 2.0. Note that the old SQLContext and HiveContext are kept for backward compatibility. A new
     * catalog interface is accessible from SparkSession - existing API on databases and tables access such as
     * listTables, createExternalTable, dropTempView, cacheTable are moved here. The below description is for old
     * hiveContext, only for knowledge. We will use sparkSession to read*/

    /* HiveContext is an alternative entry point into the Spark SQL library. It extends the SQLContext class for
     * processing data stored in Hive. It also provides a HiveQL parser. A Spark SQL application must create an
     * instance of either this class or the SQLContext class.
     *
     * HiveContext provides a superset of the functionality provided by SQLContext. The parser that comes
     * with HiveContext is more powerful than the SQLContext parser. It can execute both HiveQL and SQL queries.
     * It can read data from Hive tables. It also allows applications to access Hive UDFs (user-defined functions).
     *
     * Note that Hive is not a requirement for using HiveContext. You can use HiveContext even if you do not
     * have Hive installed. In fact, it is recommended to always use HiveContext since it provides a more complete
     * parser than the SQLContext class.
     *
     * If you want to process Hive tables or execute HiveQL queries on any data source, you must create an
     * instance of the HiveContext class. It is required for working with tables defined in the Hive metastore.
     * In addition, if you want to process existing Hive tables, add your hive-site.xml file to Spark’s classpath, since
     * HiveContext reads Hive configuration from the hive-site.xml file.
     *
     * If HiveContext does not find a hive-site.xml file in the classpath, it creates the metastore_db and
     * warehouse directories in the current directory. Therefore, you could end up with multiple copies of these
     * directories if you launch a Spark SQL application from different directories. To avoid this, it is recommended
     * to add hive-site.xml file to Spark’s conf directory if you use HiveContext*/

    // Can't make it work, need to revisit
  //  hiveContextOperation(personsDF)

  }
  def sqlContextOperation(spark:SparkSession,sourceData:DataFrame):Unit={
    /*
  * Before spark 2.0+, we need to build first a sparkContext (for spark core), then create a sqlContext based on the
  * sparkContext
  *
  * val config = new SparkConf().setAppName("App_Name")
  * val sc=new SparkContext(config)
  * val sqlContext= new SQLContext(sc)
  * */

    /* The SQLContext class provides a method named sql, which executes a SQL query using Spark. It takes a SQL
     * statement as an argument and returns the result as an instance of the DataFrame class. In spark 2.0+, we don't
     * need to use sqlContext to call sql, we can use sparkSession to call sql directly.
     *
     * To use sql function on a dataframe/dataset, we must register the dataframe/dataset as a sql temporary view.
     * Temporary views in Spark SQL are session-scoped and will disappear if the session that creates it terminates.
     * If you want to have a temporary view that is shared among all sessions and keep alive until the Spark
     * application terminates, you can create a global temporary view. Global temporary view is tied to a system
     * preserved database global_temp, and we must use the qualified name to refer it, e.g.
     * SELECT * FROM global_temp.view1.*/
    // for spark 2.0+, we can get sqlContext directly from the sparkSession
    val sqlContext = spark.sqlContext
    sourceData.createOrReplaceTempView("persons")
    //Global temp view
    sourceData.createOrReplaceGlobalTempView("globalPersons")
    //use spark session to call sql
    spark.sql("select * from global_temp.globalPersons where name=='Bob'").show
    //use sqlContext to call sql
    val sqlRes1=sqlContext.sql("select * from persons")
    //sqlRes1.show()
    val ageOf30=sqlContext.sql("select * from persons where age==30")
    ageOf30.show()
  }

def hiveContextOperation(sourceData:DataFrame):Unit={

  /*******************************4.4.2.1 Create sparkSession with hive connectivity********************************/
  //if you want to use sparkSesseion to access hive table, you need to give the location of hive warehouse
  /* You can find the warehouse location at hive-site.xml in hive/conf dir. It looks like this
  * <name>hive.metastore.warehouse.dir</name>
    <value>hdfs://127.0.0.1:9000/user/hive/warehouse</value>
*/
  //val hiveWarehouseLocation="hdfs://127.0.0.1:9000/user/hive/warehouse"
  val localWarehouse="file:///tmp/spark-warehouse"
  val spark=SparkSession
    .builder()
    .appName("spark_access_hive_table")
    //.config("hive.metastore.uris","thrift://127.0.0.1:9083")
    //.config("spark.driver.allowMultipleContexts", "true")
    // hive.metastore.warehouse.dir is depracted since spark 2.0, use spark.sql.warehouse instead
    .config("spark.sql.warehouse.dir",localWarehouse)
    //.config("spark.sql.catalogImplementation","hive")
    .enableHiveSupport()
    .getOrCreate()

  /* .enableHiveSupport() provides HiveContext functions. So you're able to use catalog functions since spark
  * has provided connectivity to hive metastore on doing .enableHiveSupport()*/

  /* I was unable to connect to a existing hive data warehouse, the above will create a local spark-warehouse in
  the local disk. Need revisit this!!!*/

  /**************************** 4.4.2.1 Write data to hive table *******************************/
/* Get metadata from the warehous catalog*/
 //spark.sql("show tables").show()
  spark.catalog.listDatabases().show(false)
  spark.catalog.listTables().show(false)
  println(s"Spark conf ${spark.conf.getAll.mkString("\n")}")
  val hiveInputDF=sourceData.coalesce(1)
  //spark.sql("DROP TABLE IF EXISTS persons")
  spark.sql("CREATE TABLE persons (Id Long, Name String, Age Int)")
  hiveInputDF.write.mode(SaveMode.Overwrite).saveAsTable("persons")

  //read tables in hive
  spark.sql("show tables").show()
  spark.sql("select * from persons").show()


}
  case class Person(Id: Long, Name: String, Age: Int)


}

