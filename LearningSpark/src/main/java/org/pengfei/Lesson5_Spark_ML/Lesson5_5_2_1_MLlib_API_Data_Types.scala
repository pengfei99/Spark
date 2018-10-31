package org.pengfei.Lesson5_Spark_ML

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.distributed._
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.linalg.{Matrices, Matrix, Vector, Vectors}
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

object Lesson5_5_2_1_MLlib_API_Data_Types {
def main(args:Array[String])={
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  val spark = SparkSession.builder().master("local[2]").appName("Lesson5_5_2_MLlib_API").getOrCreate()


  /***********************************************************************************************************
    * ************************************5.5.2 MLlib API*************************************************
    * *****************************************************************************************************/
  /*
  * The MLlib library can be used with applications developed in Scala, Java, Python, or R. This Lesson covers
  * the Scala version of the MLlib API. The classes and singleton objects provided by MLlib are available under
  * the org.apache.spark.mllib package.*/
  /**************************************5.5.2.1 Basic Data Types******************************************/
  /*
  * MLlib’s primary data abstractions are
  * - Local Vector
  * - LabeledPoint
  * - Local matrix
  * - Distributed matrix
  *   -- Row matrix
  *   -- Indexed Row matrix
  *   -- Coordinate matrix
  *   -- Block matrix
  * - Rating.
  * The machine learning algorithms and statistical utilities in MLlib operate on data represented by these
  * abstractions.*/

  //LocalMatrixExample(spark)
  DistributedMatrixExample(spark)

}

  /*****************************************************************************************************
  **************************************5.5.2.1.1 Local Vector ****************************************
    * **********************************************************************************************/
  def VectorExample(spark:SparkSession):Unit={


  /* The Local Vector type represents an indexed collection of Double-type values with zero-based index of type Int.
   * It is generally used for representing the features of an observation in a dataset. Conceptually, a Vector
   * of length n represents an observation with n features. In other words, it represents an element in an
   * n-dimensional space.
   *
   * The Vector type provided by MLlib should not be confused with the Vector type in the Scala collection
   * library. They are different. The MLlib Vector type implements the concept of numeric vector from linear
   * algebra. An application must import org.apache.spark.mllib.linalg.Vector to use the Vector trait
   * provided by MLlib.
   *
   * The MLlib library supports two types of vectors:
   * - dense
   * - sparse
   *
   * The MLlib Vector type is defined as a trait, so an application cannot directly create an instance of Vector.
   * Instead, it should use the factory methods provided by MLlib to create either an instance of the DenseVector
   * or SparseVector class. These two classes implement the Vector trait. The factory methods for creating an
   * instance of the DenseVector or SparseVector class are defined in the Vectors object.*/

  /******************************************** Dense Vector **************************************/
  /* An instance of the DenseVector class stores a double-type value at each index position. It is backed by an
   * array. A dense vector is generally used if a dataset does not have too many zero values. It can be created, as
   * shown here.*/

  val denseVec:Vector=Vectors.dense(1.0,0.0,3.0)

  /* The dense method creates an instance of the DenseVector class from the values provided to it as
   * arguments. A variant of the dense method takes an Array of Double type as an argument and returns an
   * instance of the DenseVector class. */

  val denseVec1:Vector=Vectors.dense(Array(1.0,0.0,3.0))

  /****************************************** Sparse Vector *******************************************/
  /*
  * The SparseVector class represents a sparse vector, which stores only non-zero values. It is an efficient data
  * type for storing a large dataset with many zero values. An instance of the SparseVector class is backed by
  * two arrays; one stores the indices for non-zero values and the other stores the non-zero values.*/

  //For example, I want to create a sparseVector of (1.0,0.0,3.0), the sparse method takes three arguments. 1st is the
  //size of vector, in our case is 3, the second argument is the array of index of non 0 value. the 3rd argument is the
  //the array of values.

  val sparseVec1=Vectors.sparse(3,Array(0,2),Array(1.0,3.0))

  //You can also replace two arrays by a Seq of (index,value),
  val sparseVec2=Vectors.sparse(3,Seq((0,1.0),(2,3.0)))
}

  /*****************************************************************************************************
    **************************************5.5.2.1.2 Labeled point ****************************************
    * **********************************************************************************************/
  def LabeledPointExample(spark:SparkSession):Unit={
    /* The LabeledPoint type represents an observation in a labeled dataset. It contains both the label (dependent
     * variable) and features (independent variables) of an observation. The label is stored as a Double-type value
     * and the features are stored as a Vector type.
     *
     * An RDD of LabeledPoints is MLlib’s primary abstraction for representing a labeled dataset. Both
     * regression and classification algorithms provided by MLlib operate only on RDD of LabeledPoints.
     * Therefore, a dataset must be transformed to an RDD of LabeledPoints before it can be used to train a model.
     *
     * Since the label field in a LabeledPoint is of type Double, it can represent both numerical and categorical
     * labels. When used with a regression algorithm, the label in a LabeledPoint stores a numerical value. For
     * binary classification, a label must be either 0 or 1. 0 represents a negative label and 1 represents a positive
     * label. For multi-class classification, labels should be class indices starting from zero: 0, 1, 2, ....
     * */
    /* The below code represent two observations with 1 label and 3 features, one pos and one neg, */
    val positiveRow = LabeledPoint(1.0,Vectors.dense(10.0,30.0,20.0))
    val negativeRow = LabeledPoint(0.0,Vectors.sparse(3,Array(0,2),Array(200.0,300.0)))
    /* It is very common in practice to have sparse training data. MLlib supports reading training examples stored in
     * LIBSVM format, which is the default format used by LIBSVM and LIBLINEAR. It is a text format in which each line
     * represents a labeled sparse feature vector using the following format:
     *
     * label index1:value1 index2:value2 ...
     *
     * where the indices are one-based and in ascending order. After loading, the feature indices are converted
     * to zero-based.
     * */

  }

  /*****************************************************************************************************
    **************************************5.5.2.1.3 Local matrix ****************************************
    * **********************************************************************************************/
  def LocalMatrixExample(spark:SparkSession):Unit={
    /* A local matrix has integer-typed row and column indices and double-typed values, stored on a "single machine!!!".
     * MLlib supports dense matrices, whose entry values are stored in a single double array in column-major order,
     * and sparse matrices, whose non-zero entry values are stored in the Compressed Sparse Column (CSC) format in
     * column-major order. For example, the following dense matrix
     *
     * 1.0  2.0
     * 3.0  4.0
     * 5.0  6.0
     * is stored in a one-dimensional array [1.0,3.0,5.0,2.0,4.0,6.0] with the matrix size (3,2), you could notice
     * the first three element is the column 1, the second three element is the column 2.
     * */

    /*Create a matrix:
    *  9.0   0.0
    *  0.0   8.0
    *  0.0   6.0
    *
    *  */

    val denseMatrix:Matrix=Matrices.dense(3,2,Array(9.0,0.0,0.0,0.0,8.0,6.0))
    /* sparse matrix is stored as compressed sparse column format. the sparse method takes 5 argument
     * 1st arg is row size, 2nd arg is column size, 3rd is array of ColPtr, 4th is array of rowIndex, 5th
      * is array of value
      * https://medium.com/@rickynguyen/getting-started-with-spark-day-5-36b62a6d13bf explains well how to
      * calculate */

    val sparseMatrix:Matrix=Matrices.sparse(3,2,Array(0, 1,3), Array(0, 1, 2), Array(9, 6, 8))
    println(sparseMatrix.toString())
  }

  /*****************************************************************************************************
    **************************************5.5.2.1.4 Distributed matrix *********************************
    * **********************************************************************************************/
  def DistributedMatrixExample(spark:SparkSession):Unit= {
    /* A distributed matrix has long-typed row and column indices and double-typed values, stored distributively
    * in one or more RDDs. It is very important to choose the right format to store large and distributed matrices.
    * Converting a distributed matrix to a different format may require a global shuffle, which is quite expensive.
    * Four types of distributed matrices have been implemented so far:
    * - RowMatrix
    * - IndexedRowMatrix
    * - CoordinateMatrix
    * - BlockMatrix*/

    /****************************************RowMatrix**********************************************/
    /* A RowMatrix is a row-oriented distributed matrix without meaningful row indices, e.g., a collection of
    * feature vectors. It is backed by an RDD of its rows, where each row is a local vector. We assume that the
    * number of columns is not huge for a RowMatrix so that a single local vector can be reasonably communicated
    * to the driver and can also be stored / operated on using a single node.
    *
    * A RowMatrix can be created from an RDD[Vector] instance. Then we can compute its column summary
    * statistics and decompositions. QR decomposition is of the form A = QR where Q is an orthogonal matrix
    * and R is an upper triangular matrix. For singular value decomposition (SVD) and principal component
    * analysis (PCA), please refer to Dimensionality reduction.
    *
    *
    */
   val rows=Array(Vectors.dense(1.0,2.0,3.0),
      Vectors.dense(1.0,2.0,3.0),
      Vectors.dense(1.0,2.0,3.0),
      Vectors.dense(1.0,2.0,3.0))
    //Build RDD[Vector]
    val vecRDD=spark.sparkContext.parallelize(rows)
    //Build Matrix
    val mat:RowMatrix=new RowMatrix(vecRDD)
    //Get its size
    val m=mat.numRows()
    val n=mat.numCols()
    //QR decomposition
    val qrResult=mat.tallSkinnyQR(true)
    println(s"matrix has ${m} rows, ${n} columns, qrResult is ${qrResult.toString}")

    /***********************************************IndexedRow Matrix********************************************/

    /* An IndexedRowMatrix can be created from an RDD[IndexedRow] instance, where IndexedRow is a wrapper over
     * (Long, Vector). An IndexedRowMatrix can be converted to a RowMatrix by dropping its row indices.*/
  val indexedRows=Array(IndexedRow(0,Vectors.dense(1.0,2.0,3.0)),
      IndexedRow(1,Vectors.dense(1.0,2.0,3.0)),
      IndexedRow(2,Vectors.dense(1.0,2.0,3.0)),
      IndexedRow(3,Vectors.dense(1.0,2.0,3.0)))
    val indexedRowRDD=spark.sparkContext.parallelize(indexedRows)
    val indexMat:IndexedRowMatrix=new IndexedRowMatrix(indexedRowRDD)

    // Get its size.
    val indexM = indexMat.numRows()
    val indexN = indexMat.numCols()
    println(s"matrix has ${indexM} rows, ${indexN} columns")
    //Covert it to rowMatrix, which means drop its row indices
    val rowMat:RowMatrix=indexMat.toRowMatrix()

    /*********************************************CoordinateMatrix******************************************************/
  /* A CoordinateMatrix is a distributed matrix backed by an RDD of its entries. Each entry is a tuple of
   * (i: Long, j: Long, value: Double), where i is the row index, j is the column index, and value is the entry value.
   * A CoordinateMatrix should be used only when both dimensions of the matrix are huge and the matrix is very sparse.*/

    val matrixEntries=Array(MatrixEntry(0,1,1.0),
    MatrixEntry(10,1,2.0),
    MatrixEntry(100,2,3.0)
    )
    val matrixRDD:RDD[MatrixEntry]=spark.sparkContext.parallelize(matrixEntries)

    val coordinateMat:CoordinateMatrix=new CoordinateMatrix(matrixRDD)

    //get its size
    val coordinateM = coordinateMat.numRows()
    val coordinateN = coordinateMat.numCols()
    println(s"matrix has ${coordinateM} rows, ${coordinateN} columns")
    /*********************************************BlockMatrix******************************************************/

    /* A BlockMatrix is a distributed matrix backed by an RDD of MatrixBlocks, where a MatrixBlock is a tuple of
    * ((Int, Int), Matrix), where the (Int, Int) is the index of the block, and Matrix is the sub-matrix at the
    * given index with size rowsPerBlock x colsPerBlock. BlockMatrix supports methods such as add and multiply
    * with another BlockMatrix. BlockMatrix also has a helper function validate which can be used to check
    * whether the BlockMatrix is set up properly.*/

    //We can convert a coordinateMatrix to a BlockMatrix
    val blockMat:BlockMatrix=coordinateMat.toBlockMatrix().cache()
    // Validate whether the BlockMatrix is set up properly. Throws an Exception when it is not valid.
    // Nothing happens if it is valid.
    blockMat.validate()
    // Calculate A transpose(invert col and row index) times A.
    val ata = blockMat.transpose.multiply(blockMat)
    println(s"ata result ${ata.toString}")
  }

  /*****************************************************************************************************
    **************************************5.5.2.1.5 Rating *********************************************
    * **********************************************************************************************/
  def RatingExample(spark:SparkSession):Unit={
    /*
 * The Rating type is used with recommendation algorithms. It represents a user’s rating for a product
 * or item. A training dataset must be transformed to an RDD of Ratings before it can be used to train a
 * recommendation model.
 *
 * Rating is defined as a case class consisting of three fields. The first field is named user, which is of type
 * Int. It represents a user identifier. The second field is named product, which is also of type Int. It represents
 * a product or item identifier. The third field is named rating, which of type Double.*/

    val rating=Rating(100,10,3.0)

    /* This code creates an instance of the Rating class. This instance represents a rating of 3.0 given by a user
     * with identifier 100 to a product with identifier 10.*/
  }

 }