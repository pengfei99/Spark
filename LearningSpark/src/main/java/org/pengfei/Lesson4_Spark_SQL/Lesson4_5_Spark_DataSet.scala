package org.pengfei.Lesson4_Spark_SQL

import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Encoders, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.pengfei.Lesson4_Spark_SQL.Lesson4_1_Spark_SQL_Intro.Person
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.functions._

object Lesson4_5_Spark_DataSet {

  def main(args:Array[String]):Unit={

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val spark=SparkSession.builder().master("local[2]").appName("Lesson4_5_Spark_DataFrame").getOrCreate()
    val sc=spark.sparkContext
    import spark.implicits._

  /** ***********************************************************************************************
    * ***********************************4.5 Spark sql DataSet/DataFrame ********************************
    * **********************************************************************************************/


  /**************************************4.5.1 DataSet Introduction******************************/

  /* A Spark Dataset is a group of specified heterogeneous columns, akin to a spreadsheet or a
   * relational database table. RDDs have always been the basic building blocks of Spark and
   * they still are. But RDDs deal with objects; we might know what the objects are but the
   * framework doesn't. So things such as type checking and semantic queries are not possible
   * with RDDs. Then came DataFrames, which added schemas; we can associate schemas with
   * an RDD. DataFrames also added SQL and SQL-like capabilities.
   *
   *
   * Spark 2.0.0 added Datasets, which have all the original DataFrame APIs as well as compiletime
   * type checking, thus making our interfaces richer and more robust. So now we have
   * three mechanisms:
   * - Our preferred mechanism is the semantic-rich Datasets
   * - Our second option is the use of DataFrames as untyped views in a Dataset
   * - For low-level operations, we'll use RDDs as the underlying basic distributed objects
   *
   * RDD uses java serialization or Kryo to serialize the objects
   * DataSet uses a specialized Encoder to serialize the objects. Encoders are code generated
   * dynamically and use a format that allows Spark to perform many operations like filtering,
   * sorting and hashing without deserializing the bytes back into an object.
   *
   *
   * */

    /**************************************4.5.2 DataSet Creation******************************/
    /* DataSet can be created from various source such as :
     * - Existing RDDs
     * - Structured/Semi-Structured data files
     * - External DataBases
     * - Tables in Hive
     * */

    // DSCreationOperation(spark)


    /******************************4.5.3 Processing Data with dataset API(Transformation)**************************/

    /* In spark sql api, we have two different ways to analyze/process data.
    * - Use embedded sql/hiveSql(no compile time check/ has to create view for data)
    * - Use dataset/dataframe api.
    * The dataset api can be divided into two categories transformation, action
    * In transformation, we have
    * - General transformation : map, filter, flatMap, mapPartitions, mapPartitionsWithIndex, groupBy, cube, rollup,
    *                            sortBy, select
    *
    * - Math/Statistical transformation : sample, randomSplit
    *
    * - Set/Relational transformation : union, intersection, subtract, distinct, cartesian, zip, join.
    *
    * - DataStructure/I/O transformation : keyBy, zipWithIndex(m), zipWithUniqueID(m), zipPartitions(m),
    *                                      coalesce, repartition, repartitionAndSortWithinPartions(m), pipe(m)
    *
    * The dataset api provides 5 types of operations, we will illustrate them all in the following code
    *
    * */

    // DSTransformationOperation(spark)


    /******************************4.5.4 Processing Data with dataset API (Action)**************************/
    /* In actions, we have:
    * General action : reduce, collect, aggregate, fold, first, take, forEach, top, treeAggregate(m), treeReduce,
    *                  forEachPartition, collectAsMap
    *
    * Math/Statistical action :  count, takeSample, max, min, sum, histogram(m), mean, variance, setdev,
    *                            sampleVariance(m), countApprox(m), countApproxDistinct(m)
    *
    * Set Theory/Relational action : takeOrdered
    *
    * DataStructure/I/O action : saveAsTextFile, saveAsSequenceFile, saveAsObjectFile, saveAsHadoopDataSet,
    *                            saveAsHadoopFile, saveAsNewAPIHadoopDataSet, saveAsNewAPIHadoopFile
    *
    * */

   //DSActionOperations(spark)

    /********************************* 4.5.5 DataSet to rdd **********************************************/
  // DSRDDOperations(spark)

   /********************************* 4.5.6 Dataset Built-in Functions ******************************************/
   /*The built-in function examples are in the Lesson4_6 */

    /**********************************4.5.7 DataSet output *********************************************/
   // DSOutputOperations(spark)

  }

  def DSCreationOperation(spark:SparkSession):Unit={

    /***********4.5.2.1 Create DataSet with RDDs***************/
    val sc=spark.sparkContext
    import spark.implicits._
    val personRdd=sc.parallelize(List(
      Person(0, "Alice", 30),
      Person(1, "Bob", 30),
      Person(2, "Charles", 21),
      Person(3, "Defence", 20),
      Person(4, "Eletro", 58),
      Person(5, "Bob", 40)))

    /* The simplest way is to use spark implicit conversion*/
    val personDS=personRdd.toDS()
    //println(s"personDS value is : ${personDS.collect().mkString(",")}")
    personDS.show()
    /* CreateDataSet() is a good way to create a Dataset from an RDD.*/
    val personDS1=spark.createDataset(personRdd)
    //println(s"personDS1 has type: ${personDS1.getClass.getName}")
    //println(s"personDS1 value is : ${personDS1.collect().mkString(",")}")
    personDS1.show()

    /* We can use Encoders to describe the schema of strongly-typed datasets (only works with types such as
     * INT, case class, etc.). To check the schema of dataset objects, we can use .schema*/
    //Encoders.INT.schema.printTreeString()
    //Encoders.product[Person].schema.printTreeString
    //println(s"The schema of personDS1 : ${personDS1.schema}")

    val rawRdd=sc.parallelize(List(
      Row(0, "Alice", 30),
      Row(1, "Bob", 30),
      Row(2, "Charles", 21),
      Row(3, "Defence", 20),
      Row(4, "Eletro", 58),
      Row(5, "Bob", 40)
    ))

    /* We have seen how to convert a rdd of case class object(structure data) to dataset, now we will try to
     * give a schema of semi-structure data
     *
     * The schema for a dataset can be specified with an instance of StructType, which is a case class.
     * A StructType object contains a sequence(e.g. Seq, Array) of StructField objects. StructField is
     * also defined as a case class. It is used to specify the name and data type of a column, and optionally
     * whether a column can contain null values and its metadata. The types of structField are defined in
     * org.apache.spark.sql.types._ */


    val personSchema=StructType(Array(
      StructField("Id", LongType, true),
      StructField("Name", StringType, true),
      StructField("Age", IntegerType, true)
    ))

    /* StructType offers printTreeString method that print the schema in tree form.*/
    //personSchema.printTreeString()
    //The createDataset method can't take rowRDD as input, it can only take not row rdd.
    //val personDS2=spark.createDataset(rawRdd)
    //println(s"personDS2 schema is : ${personDS2.schema}")

    /*To add schema, we have to use createDataFrame, because all createDataset did not use schema. The createDataFrame
     * method which can use schema requires the rdd must be a rdd or rows(RDD[row])*/
    val personDS3=spark.createDataFrame(rawRdd,personSchema)
    println(s"personDS3 schema is : ${personDS3.schema}")

    /***********4.5.2.2 Create DataSet with files***************/
    /* */
    /* Read csv with a schema*/
    val personCSVPath="/DATA/data_set/spark/basics/Lesson4_Spark_SQL/person.csv"
    //val personHdfsPath="hdfs://<namenode-ip>:<port>/path/to/file"
    //val personS3Path="s3a://<bucket_name>/path/to/file"
    /* The format option specifies the file format, it can be csv, parquet, orc,JSON
     * - parquet : org.apache.spark.sql.parquet
     * - csv : org.apache.spark.sql.csv
     * - json : org.apahce.spark.sql.json
     * - orc : org.apahce.spark.sql.orc
     *
     * The loar option specifies the file path, it can be local file, remote hdfs, s3. etc.*/
    val personDS4=spark.read.format("csv").option("delimiter",",").schema(personSchema).load(personCSVPath)
    personDS4.show()
    println(s"personDS4 schema is : ${personDS4.schema}")

    /* Read csv with column name header, without schema, let the spark read method to infer a schema automaticlly*/
    val personCSVWithHeadPath="/DATA/data_set/spark/basics/Lesson4_Spark_SQL/person_with_head.csv"
    val personDS5=spark.read.option("header","true").option("inferSchema","true").csv(personCSVWithHeadPath)
    personDS5.show()
    println(s"personDS5 schema is : ${personDS5.schema}")


    /* Read files in all format */
    /*val personParquetPath="/DATA/data_set/spark/basics/Lesson4_Spark_SQL/person.parquet"
    val personDS6=spark.read.option("header","true").option("inferSchema","true").parquet(personParquetPath)
    personDS6.show()
    println(s"personDS6 schema is : ${personDS6.schema}")

    val personOrcPath="/DATA/data_set/spark/basics/Lesson4_Spark_SQL/person.orc"
    val personDS7=spark.read.option("header","true").option("inferSchema","true").orc(personOrcPath)
    personDS7.show()
    println(s"personDS7 schema is : ${personDS7.schema}")

    val personJsonPath="/DATA/data_set/spark/basics/Lesson4_Spark_SQL/person.json"
    val personDS8=spark.read.option("header","true").option("inferSchema","true").json(personJsonPath)
    personDS8.show()
    println(s"personDS8 schema is : ${personDS8.schema}")*/

    /* Use sql query on parquet file directly, */
    //val personDS6=spark.sql("SELECT * FROM parquet.'/DATA/data_set/spark/basics/Lesson4_Spark_SQL/person.parquet'")

    /***********4.5.2.3 Create DataSet from database***************/
    /* Spark SQL has built-in support for JDBC-compliant databases. External packages are available for
     * other data sources.*/

    /*val jdbcDf=spark.read.format("org.apache.spark.sql.jdbc").options(Map(
       "url" -> "jdbc:postgresql://127.0.0.1:5432/northwind?user=pliu&password=changeMe",
       "dbtable" -> "public.employees"
     )).load()

    jdbcDf.show(5)*/

    // We can also use option for each connection info
   /* val taoBaoDF=spark.read.format("jdbc")
      .option("url","jdbc:postgresql://127.0.0.1:5432/dbtaobao")
      .option("dbtable","public.user_log")
      .option("user","pliu")
      .option("password","changeMe").load()
    taoBaoDF.show()*/

    /* We can also use the following syntax*/
    /*val connectionProperties= new Properties()
    connectionProperties.put("user","pliu")
    connectionProperties.put("password","changeMe")
    val taoBaoDF1=spark.read.jdbc("jdbc:postgresql://127.0.0.1:5432/dbtaobao","public.user_log",connectionProperties)
    taoBaoDF1.show(5)*/


    /****************4.5.2.4 Create DataSet from hive table*********************/
    /* The following code for read hive table are not tested*/
    /* First, Create a spark session which support hive.
    * Then there are two ways to read hive table:
     * - use sparkSession.table method -> returns all data of one table as a dataset
     * - use sparkSession.sql("sql statement") -> returns the result data of the sql statement as a dataset*/
    /*val localWarehouse="file:///tmp/spark-warehouse"
    val sparkHive=SparkSession
      .builder()
      .appName("spark_access_hive_table")
      .config("hive.metastore.uris","thrift://127.0.0.1:9083")
      // hive.metastore.warehouse.dir is depracted since spark 2.0, use spark.sql.warehouse instead
      .config("spark.sql.warehouse.dir",localWarehouse)
      //.config("spark.sql.catalogImplementation","hive")
      .enableHiveSupport()
      .getOrCreate()

    val df1 = sparkHive.read.table("hive-table-name")
    val df2 = sparkHive.sql("select * from hive-table-name")*/
  }

  def DSTransformationOperation(spark:SparkSession):Unit={
    import spark.implicits._
    val personParquetPath="/DATA/data_set/spark/basics/Lesson4_Spark_SQL/person.parquet"
    val personDS=spark.read.option("header",true).option("inferSchema","true").parquet(personParquetPath)
    personDS.show()

    val sales = Seq(
      ("Warsaw", 2016, 100),
      ("Warsaw", 2017, 200),
      ("Boston", 2015, 50),
      ("Boston", 2016, 150),
      ("Toronto", 2017, 50),
      ("Toronto", 2017, 50)
    ).toDF("city", "year", "amount")

    val sales1=Seq(
      ("Beijin", 2016, 100),
      ("Warsaw", 2017, 200),
      ("Boston", 2015, 50),
      ("Benxi", 2016, 150),
      ("Toronto", 2017, 50),
      ("GuangZhou", 2017, 50)
    ).toDF("city", "year", "amount")

    /**************************************4.5.3.1 Basic operations for exploring data *********************/

    /**************************************Cache/persist dataset**********************************/
    /*The cache method stores source dataset in memory using a columnar format. It scans only the required
    * columns and stores them in compressed in-memory columnar format. Spark automatically selects a compress
    * codec for each column based on data statistics*/
    personDS.cache()
    /* We can tune the spark caching by adding the following option*/
    spark.conf.set("spark.sql.inMemoryColumnarStorage.compressed","true")
    spark.conf.set("spark.sql.inMemoryColumnarStorage.batchSize","10000")
    /* By default, compression is turned on and the batchSize is 10,000*/

    /* With cache(), we can only use the default(memory_only) storage level. With persist(), you can specify
    * many different storage level. The following example, we use memory and disk to store the dataset with a
    * serilization object*/

    //personDS.persist(StorageLevel.MEMORY_AND_DISK_SER_2)

    /**********************************Columns and rows of dataset ************************/
    /* Row is a Spark Sql abstraction for representing a row of data. Conceptually, it's equivalent to a relational
     * tuple or row in a table */
    /* An example of row of an object*/
    val row1=Row(Person(6,"haha",28))

    val p1=row1.get(0)
    println(s"Person p1 from row 1 has value: ${p1.toString}")

    /* An example of row of string*/
    val row2=Row(7,"foo",18)
    val pid=row2.getInt(0)
    val pname=row2.getString(1)
    val page=row2.getInt(2)
    println(s"Person p2 form row2 has value: pid: ${pid}, pname: ${pname}, page: ${page}")

    /* The columns method returns the names of all the columns in the dataset as an array of String*/
    val columns=personDS.columns
    println(s"Columns name of personDS: ${columns.mkString(",")}")

    /***********************************Dtypes of dataset ***************************/
    /* dtypes method returns the data types of all the columns in the dataset as an array of tuples. The
     * first element of the tuple is the name, second element is the type of a column */
    val columnsWithType =personDS.dtypes
    println(s"Columns with types of personDS: ${columnsWithType.mkString(",")}")

    /******************************** Explain *************************************/
    /* The explain method prints the physical plan on the console. It is useful for debugging*/
    //personDS.explain(false)

    /***************************** printSchema ************************************/
    /* The printSchema method prints the schema of the source DataFrame on the console in a tree format*/
    personDS.printSchema()

    /**********************************toDF****************************************/
      /* We can use toDF to rename column name, it's better to use withColumns if you need to do more on the column*/
    val renameDS= personDS.toDF("SId","SName","SAge")
    renameDS.printSchema()

    /**************************************4.5.3.2 Transformation operations *************************************/

    /****************************************Distinct*********************************************************/
    /* The distinct method returns a new Dataframe containing only the unique rows in the source dataset*/

     val salesWithoutDuplicates = sales.distinct()
    salesWithoutDuplicates.show()


    /**************************************** filter **************************************************/

   /* The filter method filters rows in the source DataSet using a sql expression provided to it as an argument.
    * It returns a new DataSet containing only the filtered rows. The SQL expression can be passed as a string argument */

    val filteredSales=sales.filter("amount > 100")
    filteredSales.show()

    // A variant of the filter method allows a filter condition to be specified using the Column type. you can also
    // replace the $"year" by sales("year")
    val filteredSales1=sales.filter($"year" > 2010)
    filteredSales1.show()



    /******************************************Intersect****************************************************/
      /* The intersect method takes a DataFrame as an argument and returns a new DataFrame containing only
       * the rows in both the input and source DataFrame.*/
    val commonRows=sales.intersect(sales1)
    commonRows.show()

    /******************************************Join*******************************************************/
    /* Join method performs a SQL join of the source DataFrame with another DataFrame. It takes three
     * arguments, a DataFrame, a join expression and a join type.*/
    val customer=List(Customer(11, "Jackson", 21, "M"),
      Customer(12, "Emma", 25, "F"),
      Customer(13, "Olivia", 31, "F"),
      Customer(4, "Jennifer", 45, "F"),
      Customer(5, "Robert", 41, "M"),
      Customer(6, "Sandra", 45, "F")).toDF()

    val transactions= List(Transaction(1, 5, 3, "01/01/2015", "San Francisco"),
      Transaction(2, 6, 1, "01/02/2015", "San Jose"),
      Transaction(3, 1, 6, "01/01/2015", "Boston"),
      Transaction(4, 200, 400, "01/02/2015", "Palo Alto"),
      Transaction(6, 100, 100, "01/02/2015", "Mountain View")).toDF()
// the join type can be inner, outer, left_outer, right_outer,
    val innerDF = transactions.join(customer, transactions("cId")===customer("cId"),"inner")

    innerDF.show()

    /*******************************************Limit *******************************************************/
    /* The limit method returns a dataset containing the specified number of rows from the source dataset. */

    val fiveCustomer=customer.limit(5)
    fiveCustomer.show

    /*****************************************orderBy******************************************************/
    /* The orderBy method returns a DataFrame sorted by the given columns. It takes the names of one or more
     * columns as arguments.
     *
     * By default, the orderBy sorts in ascending order. You can explicitly specify the sorting order by adding
     * .desc or .asc*/

     val sortCustomerAge=customer.orderBy("age")
     sortCustomerAge.show()
    // sort with multi column
    customer.orderBy($"cId".asc,$"age".desc).show()

    /********************************************randomSplit*********************************************/
    /* The randomSplit method splits the source dataset into multiple datasets. It takes an array of weights as
    * argument and returns an an array of datasets. It is a useful method for split dataset into
    * training data and test data for machine learning.*/
    val splitCustomerDS = customer.randomSplit(Array(0.4,0.3,0.3))
    println(s"Split customer dataset, fist part count: ${splitCustomerDS(0).count}, " +
      s"2nd part count: ${splitCustomerDS(1).count}, 3rd part count: ${splitCustomerDS(2).count}")


    /***************************************************sample********************************************/
    /* The sample method returns a DataFrame containing the specified fraction of the rows in the source
     * DataFrame. It takes two arguments. The first argument is a Boolean value indicating whether sampling should
     * be done with replacement. The second argument specifies the fraction of the rows that should be returned.*/

    val sampleSales=sales.sample(true,0.10)
    //sampleSales.show()

    /**************************************************select/selectExpr *********************************************/
    /* The select method returns a DataFrame containing only the specified columns from the source DataFrame. You can
     * also modify the value of columns with select. For more complexe functions, we can use select Expr. The
     * selectExpr method accepts one or more SQL expressions as arguments and returns a DataFrame generated by
     * executing the specified SQL expressions.
     * */

    println("********************************select/selectExpr***************************************")
    val nameAgeCustom=customer.select($"name",$"age"+10)
    nameAgeCustom.show()

    val newCustomerDF=customer.selectExpr("name","age+10 AS age_10","IF(gender='F', true, false) As female")
    newCustomerDF.show()

    /******************************************** withColumn ***********************************************/
    /* The withColumn method adds a new column to or replaces an existing column in the source DataSet and returns a
    * new dataset. It takes two arguments, the first argument is the name of the new column and the second argument
    * is an expression for generating the value of the new column. */

    // if the expression is simple, you can put it directly in the withColumn method
   println("*************************************withColumn***********************************************")
    val productDS=List(Product(1,"iPhone",600.00,400.00),
                       Product(2,"iPad",400.00,300.00),
                       Product(1,"Dell",500.00,400.00)).toDF()

    val productProfit=productDS.withColumn("profit",$"price"-$"cost")
    productProfit.show()

    /*if the logic in the expression is complex, you can define a function then call the function
    * To do this there are three steps.
    * 1. define the function
    * 2. register the function to spark udf
    * 3. call the function in withColumn with funciton expr (You need to import org.apache.spark.sql.functions.expr)
    * */

    /* Step 1*/
    def getProfit(price:Double,cost:Double):Double={
      return price-cost
    }

    /*Step 2, first argument is the name of the registered function in udf. second is the lambda exp
    * which call the real function.*/
    spark.udf.register("getProfit",(price:Double,cost:Double)=>getProfit(price,cost))

    /*Step 3.  In the expr, you can use directly the column name as argument in the udf registered function*/
    val productProfitUdf=productDS.withColumn("profit",expr("getProfit(price,cost)"))
    productProfitUdf.show()




  }




  def DSActionOperations(spark:SparkSession):Unit={
   import spark.implicits._
    val sales = List(SalesSummary("01/01/2015", "iPhone", "USA", 40000),
      SalesSummary("01/02/2015", "iPhone", "USA", 30000),
      SalesSummary("01/01/2015", "iPhone", "China", 10000),
      SalesSummary("01/02/2015", "iPhone", "China", 5000),
      SalesSummary("01/01/2015", "S6", "USA", 20000),
      SalesSummary("01/02/2015", "S6", "USA", 10000),
      SalesSummary("01/01/2015", "S6", "China", 9000),
      SalesSummary("01/02/2015", "S6", "China", 6000)
    ).toDF()
    sales.show()
    /************************************** 4.5.4.1 Basic Actions **********************************/
    /************************************Apply***********************************************/
    /* The apply method takes the name of a column as an argument and returns the specified column in the source dataset
    * as an instance of Column class. The Column class provides operators for manipulating a column in a DataSet*/

    val revenueColumn=sales.apply("revenue")
    println(s"revenueColumn has value : ${revenueColumn.toString()}")
    val halfRevenueColumn=revenueColumn*0.5
    println(s"HalfRevenueColumn has value : ${halfRevenueColumn.toString()}")
    /* Scala provides syntactic sugar that allows you to use sales("year") instead of sales.apply("year").
     * It automatically converts sales("year") to sales.apply("year"). So the preceding code can be
     * rewritten, val yearColumnMinus10=sales("year")-10
     *
     * Column class is generally used as an input to some of the dataFrame methods or functions defined in the spark SQL
     * library.
     * For example sales.sum("amount") can also be written as sum(sales.apply("amount")), sum(sales("amount")) or
     * sum($"amount")*/

    /******************************************* Collect **************************************************/
    /* The collect method reutrns the data in a dataset as an array of Rows. Be careful, when you do collect with
     * big dataset. it may explose your sparkdriver memory when all the workers sends back results. */

      //println(s"result of sales after collect: ${sales.collect().mkString(",")}")

    /*********************************************count ******************************************************/
    /* Count method returns the number of rows in the dataset*/
    val count=sales.count()

    /********************************************describe*********************************************/
    /* The describe method can be used for exploratory data analysis. It returns summary statistics for numeric columns
    * in the source dataset. The summary statistics includes min, max, count, mean, and standard deviation. It can take
    * one or more column as arguments and return the summary as a dataset*/
    sales.describe("revenue").show()

    /*******************************************first/take**********************************************/
    /* The first method returns the first row in the source DataFrame*/
    val firstRow=sales.first()
    /* The take method takes an integer N as an argument and returns the first N rows from the source DataFrame
     * as an array of Rows.*/
    val firstTwoRows= sales.take(2)

    /*****************************************groupby**************************************************/
    /* The groupBy method groups the rows in the source dataset using the columns provided to it as arguments.
    * Aggregation can be performed on the grouped data returned by this method
    *
    * The aggregation method can be count, min, max, sum,*/
    val countCity = sales.groupBy("country").count()
    countCity.show()
    val salesAmountOfCity= sales.groupBy("country").sum("revenue")
    salesAmountOfCity.show()

    /********************************************* Cube *****************************************************/

    /* The cube method takes the names of one or more columns as arguments and returns a cube for multi-dimensional
    * analysis. It is useful for generating cross-tabular reports.
    *
    * Assume you have a dataset that tracks sales along three dimensions: time, product and country. The cube method
    * allows you to generates for all possible combinations of the dimensions that you are interested in.
    *
    * It works like a super groupby, it does groupby on all the target column and return all possible groupby result
    * In the following example, it groupby date, product, country. The fist row of the result dataset
    * |01/01/2015|   null|    USA|     60000.0| is a groupby of rows where date=01/01/2015 and country=USA, as the
    * product countains both iPhone and S6 so it returns null. The sum("revenue") is the sum of revenue of all
    * rows where date=01/01/2015 and country=USA */

    val salesCubeDf=sales.cube($"date",$"product",$"country").sum("revenue")
    salesCubeDf.show()

    /* If you want to find the total sales of all products in the USA, you can use the following filter query*/
    salesCubeDf.filter("product IS null AND date IS null AND country='USA'").show()
    /* If you want to know the subtotal of sales by product in the USA, you can use the following filter query*/
    salesCubeDf.filter("date IS null AND product IS NOT null AND country='USA'").show()


    /****************************************rollup***********************************************/
    /* The rollup method takes the names of one or more columns as arguments and return a multi-dimensional
    * rollup. It is useful for sub-aggregation along a hierarchical dimension such as geography or time*/
    val salesByCity = List(SalesByCity(2014, "Boston", "MA", "USA", 2000),
      SalesByCity(2015, "Boston", "MA", "USA", 3000),
      SalesByCity(2014, "Cambridge", "MA", "USA", 2000),
      SalesByCity(2015, "Cambridge", "MA", "USA", 3000),
      SalesByCity(2014, "Palo Alto", "CA", "USA", 4000),
      SalesByCity(2015, "Palo Alto", "CA", "USA", 6000),
      SalesByCity(2014, "Pune", "MH", "India", 1000),
      SalesByCity(2015, "Pune", "MH", "India", 1000),
      SalesByCity(2015, "Mumbai", "MH", "India", 1000),
      SalesByCity(2014, "Mumbai", "MH", "India", 2000)).toDF()
    val salesRollup=salesByCity.rollup($"country",$"state",$"city").sum("revenue")
    salesRollup.show()

    val salesCube=salesByCity.cube($"country",$"state",$"city").sum("revenue")
    salesCube.show()

    val salesGroupBy=salesByCity.groupBy($"country",$"state",$"city").sum("revenue")
    salesGroupBy.show()

    /**************************GroupBy vs cube vs Rollup ***********************************/
    /* We can notice that the functionality and result of groupBy, cube, rollup are very similar. They are all
     * inspired from the sql.
     *
     * groupBy : is used to group the results of aggregate functions according to a specified column. But it does not
     *           perform aggregate operation on multiple levels of a hierarchy of columns. For example, you can
     *           calculate the total of all employee salaries for each department in a company (one level of hierarchy)
     *           but you cannot calculate the total salary of all employees regardless of the department they work in
     *           (two levels of hierarchy).
     *
     * rollup  : extend the functionality of groupBy by calculating subtotals and grand totals for a set of
     *           columns with respect of hierarchy(column orders)
     *
     * cube    : cube is similar to rollup, but it will calculate all possible subtotals and grand totals for
     *           all possible permutations of the columns*/



  }

  /**********************************************************************************************************
    * **************************************************4.5.5 DataSet to rdd***********************************
    * ************************************************************************************************/
  def DSRDDOperations(spark: SparkSession):Unit={
    val sc=spark.sparkContext
    import spark.implicits._

    val customerDF=List(Customer(11, "Jackson", 21, "M"),
      Customer(12, "Emma", 25, "F"),
      Customer(13, "Olivia", 31, "F"),
      Customer(4, "Jennifer", 45, "F"),
      Customer(5, "Robert", 41, "M"),
      Customer(6, "Sandra", 45, "F")).toDF()
/* The DataFrame class supports commonly used RDD operations such as map, flatMap, foreach,
* foreachPartition, mapPartition, coalesce, and repartition. These methods work similar to their
* namesake operations in the RDD class.
*
* In addition, if you need access to other RDD methods that are not present in the DataFrame class, you
* can get an RDD from a DataFrame. This section discusses the commonly used techniques for generating an
* RDD from a DataFrame.*/

    // convert dataset to rdd
    val customerRdd= customerDF.rdd

    // get firstRow of an rdd
    val firstRow=customerRdd.first()

    // get Customer name, second element of the tuple
    val name=firstRow.getString(1)

    // get Age, third element of the tuple
    val age=firstRow.getInt(2)

    // get an rdd with only name and age, Rdd is not strongly typed, here we must use Row instead of Customer
    val NameAge=customerRdd.map{case Row(cId:Long,name:String,age:Int,gender:String)=> (name,age)}
    // the below code does not work.
    //val NameAge=customerRdd.map{case Customer(cId:Long,name:String,age:Int,gender:String)=> (name,age)}
    println(s"NameAge has value: ${NameAge.collect().mkString(",")}")



  }


  /**********************************************************************************************************
    * *********************************************4.5.7 DataSet output operations**************************
    * ************************************************************************************************/
def DSOutputOperations(spark:SparkSession):Unit={
  import spark.implicits._
  val personDS=spark.sparkContext.parallelize(List(
    Person(0, "Alice", 30),
    Person(1, "Bob", 30),
    Person(2, "Charles", 21),
    Person(3, "Defence", 20),
    Person(4, "Eletro", 58),
    Person(5, "Bob", 40))).toDF()

  /*****************************************4.5.7.1 Write dataset on disks as files***************************/
  /* The dataset write methode can write dataset in files with format json, parquet, orc, csv, .
  * While saving a dataset, if the destination path or table
  * already exists, spark sql will throw an class. You can use the mode method in DataSetWriter to change this.
  * It takes an argument saveMode which specifies the behavior if the destination already exists.
  * There are four SaveMode:
  * - error (default) : throw an exception if destination exists.
  * - append : append to existing data if exists.
  * - overwrite : overwrite existing data
  * - ignore : ignore the write operation if exists.
  *
  * */

  /*personDS.coalesce(1).write.mode(SaveMode.Overwrite).json("/tmp/personDS")
  personDS.coalesce(1).write.mode(SaveMode.Overwrite).parquet("/tmp/personParquet")
  personDS.coalesce(1).write.mode(SaveMode.Overwrite).orc("/tmp/personORC")*/

  /* The output of write method is a directory, not a single file. The number of files which contains the real data is
  * depends on the partitions of the dataset. There are three method can change the output file number(partition number)
  * - coalesce(partitionNum:Int) : can only reduce the partition number
  *
  * - repartition(partitionNum:Int,col:Column) : can both increase and reduce partition number. The col argument
  *                                              will be used as a partition key. The partition key can help spark to
  *                                              determine each row belongs to which partition. This can increase
  *                                              the analytics speed. The two argument are both optional.
  *
  * - partitionBy(colName:String) : Let spark determine the best partition number by using the colName as partition key
  *                                 It only increase the partition number, For RDD only
  *
  * We commonly use date or time column to partition data
  * */
//  personDS.repartition(6,$"Id").write.mode(SaveMode.Append).text("/tmp/personDSText")

  // we can also use save method to write files
//  personDS.write.format("parquet").mode(SaveMode.Overwrite).save("/tmp/savePersonParquet")

  /*****************************************4.5.7.2 Write dataset on database***************************/

/*The simplest way is to use save method, we use format jdbc, and option method to specify connection information. The
* SaveMode is required if you will write existing table to avoid exception*/

  /*personDS.write.format("jdbc")
    .option("url","jdbc:postgresql://127.0.0.1:5432/dbtaobao")
    .option("dbtable","public.test_person")
    .option("user","pliu")
    .option("password","changeMe").mode(SaveMode.Ignore).save()*/

  /* Second way is to use options method to specify connection information */
 /* personDS.write.format("jdbc")
    .options(Map("url" -> "jdbc:postgresql://127.0.0.1:5432/dbtaobao?user=pliu&password=changeMe",
      "dbtable" -> "public.test_person")).mode(SaveMode.Overwrite).save()*/

  /* Third way is to use write.jdbc method, it takes three arguments, 1st is the url of db, 2nd is the schema.table_name,
  * 3rd is the connection properties*/
  /*val connectionProperties= new Properties()
    connectionProperties.put("user","pliu")
    connectionProperties.put("password","changeMe")

  personDS.write.mode(SaveMode.Append)
    .jdbc("jdbc:postgresql://127.0.0.1:5432/dbtaobao","public.test_person",connectionProperties)*/
/* TroubleShooting
* - The JDBC driver class must be visible to the primordial class loader on the client session and on all executors.
*   This is because Java’s DriverManager class does a security check that results in it ignoring all drivers not
*   visible to the primordial class loader when one goes to open a connection. One convenient way to do this is
*   to modify compute_classpath.sh on all worker nodes to include your driver JARs.
*
* - Some databases, such as H2, convert all names to upper case. You’ll need to use upper case to refer to
*   those names in Spark SQL.
*   */

  /*****************************************4.5.7.3 Write dataset on hive metastore***************************/
  /* To read or write data on hive, we need to include all hive dependencies in spark driver and all worker node.
   * Because these dependecies will be needed in worker node to access the hive serialization and deserialization lib
   * in oreder to access hive datastore*/

  /*The below code are not tested!!!!!! */

  /* We need also enable hive support in the spark session*/
  /*val spark=SparkSession.builder()
    .appName("spark Hive example")
    .config("spark.sql.warehouse.dir","/tmp/spark-warehouse")
      .enableHiveSupport()
       .getOrCreate()*/

  //save dataset to hive table
 // personDS.write.mode(SaveMode.Overwrite).saveAsTable("hive_table_name")
}







  case class Person(Id: Long, Name: String, Age: Int)
  case class EmailStringBody(sender:String, recepient:String,subject:String,body:String)
  case class EmailArrayBody(sender: String, recepient: String, subject: String, body: Array[String])
  case class SalesSummary(date:String,product:String,country:String,revenue:Double)
  case class Customer(cId:Long,Name:String,age:Int,gender:String)
  case class Transaction(tId: Long, cId: Long, prodId: Long, date: String, city: String)
  case class SalesByCity(year: Int, city: String, state: String,
                         country: String, revenue: Double)
  case class Product(id:Int,name:String,price:Double,cost:Double)
}



