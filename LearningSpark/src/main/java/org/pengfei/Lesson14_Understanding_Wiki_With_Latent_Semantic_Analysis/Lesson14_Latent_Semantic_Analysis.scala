package org.pengfei.Lesson14_Understanding_Wiki_With_Latent_Semantic_Analysis


import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.ml.feature.{CountVectorizer, IDF, RegexTokenizer, StopWordsRemover}
import org.apache.spark.ml.linalg.{Vector => MLVector}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.{Matrix, SingularValueDecomposition, Vectors, Vector => MLLibVector}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ArrayBuffer


object Lesson14_Latent_Semantic_Analysis {

  /* Origin book maven repo https://github.com/sryza/aas/tree/master/ch06-lsa/src/main */

  /********************************************************************************************************************
    * ************************************************14.1 Introduction ********************************************
    * ***************************************************************************************************************/

  /* Most of the work in data engineering consists of assembling heteogenous data into some sort of queryable format.
  * We can query structured data with formal language. For example, if the structured data is tabular, we can use SQL.
  * Making tabular data accessible is often straightforward - pull data from a variety of data sources into a single
  * table, perhaps cleaning or fusing intelligently along the way.
  *
  * Unstructured text data presents a whole different set of challenges. The process of preparing data into a format
   * that humans can interact with is not so much "assembly", but rather "indexing" in the nice case or "coercion"
   * when things get ugly. A standard search index permits fast queries for teh set of documents that contains a
   * given set of terms. Sometimes, however, we want to find documents that relate to the concepts surrounding a
   * particular word even if the documents do not contain that exact string. Standard search indexes often fail to
   * capture the latent structure in the text's subject matter.
   *
   * Latent semantic analysis (LSA) is a technique in natural language processing and information retrieval that seeks
   * to better understand a corpus of the documents and the relationships between the words in those documents. It
   * attempts to distill the corpus into a set of relevant concepts. Each concept captures a thread of variation in the
   * data and often corresponds to a topic that the corpus discusses. Each concept consists of three attributes:
   * - a level of affinity for each document in the corpus
   * - a level of affinity for each term in the corpus
   * - an importance score reflecting how useful the concept is in the describing variance in the data set.
   *
   * For example, LSA might discover a concept with high affinity for the terms "Asimov" and "robot". FYI, Issac Asimov
   * is a writer who wrote many science fiction stories about robots(e.g. "foundation series"). LSA might also discover
   * high affinity for the documents "foundation series" and "science fiction". By selecting only the most important
   * concepts, LSA can throw away some irrelevant noise and merge co-occurring strnads to come up with a simpler
   * representation of the data.
   *
   * We can employ this concise representation in a variety of tasks. It can provide scores of similarity between terms
   * and other terms, between documents and other documents, and between terms and documents. By encapsulating the
   * patterns of variance in the corpus, it can base scores on a deeper understanding than simply on counting
   * occurrences and co-occurrences of words. These similarity measures are ideal for tasks such as finding the set of
   * documents relevant to query terms, grouping documents into topics, and finding related works.
   *
   * LSA discovers this lower-dimensional representation using a linear algebra technique called SVD. SVD can be thought
   * of as a more powerful version of the ALS factorization described in Lesson11_Product_Recommendation. It starts with
   * a document-term matrix generated through counting word frequencies for each document. In this matrix, each document
   * corresponds to a column, each term corresponds to a row, and each element represents the importance of a word to a
   * document. SVD then factorizes this matrix into three matrices, one of which expresses concepts in regard to
   * documents, one expresses concepts in regard of terms, and last one contains the importance for each concept.
   * The structure of these matrices can achieve a low-rank approximation of the original matrix by removing a set of
   * rows and columns corresponding to the least important concepts. That is, the matrices in this low-rank
   * approximation can be multiplied to produce a matrix close to the original, with increasing loss of fidelity as
   * each concept is removed.
   *
   * In this Lesson, we’ll embark upon the modest task of enabling queries against the full extent of human knowledge
   * based on its latent semantic relationships. More specifically, we’ll apply LSA to a corpus consisting of the full
   * set of articles contained in Wikipedia, which is about 46 GB of raw text. We’ll cover how to use Spark for
   * preprocessing the data: reading it, cleansing it, and coercing it into a numerical form. We’ll show how to compute
   * the SVD and explain how to interpret and make use of it.
   *
   * SVD(singular-value decomposition) has wide applications outside LSA. It appears in such diverse places as detecting
   * climatological trends (Michael Mann’s famous “hockey-stick” graph), face recognition, and image compression. check
   * (https://en.wikipedia.org/wiki/Singular_value_decomposition) for more details
   * Spark’s implementation can perform the matrix factorization on enormous data sets, which opens up the technique
   * to a whole new set of applications
   *
   * */

  /********************************************************************************************************************
    * ************************************** 14.2 The Document-Term Matrix ********************************************
    * ***************************************************************************************************************/

  /* Before performing any analysis, LSA requires transforming the raw text of the corpus into a document-term matrix.
  * In this matrix, each column represents a term that occurs in the corpus, and each row represents a document.
  * Loosely, the value at each position should correspond to the importance of the column’s term to the row’s document.
  * A few weighting schemes have been proposed, but by far the most common is term frequency times inverse document
  * frequency, or TF-IDF. Here’s a representation in Scala code of the formula. We won’t actually end up using this
  * code because Spark provides its own implementation.
  *
  * */
  def termDocWeight(termFrequencyInDoc: Int, totalTermsInDoc: Int,
                    termFreqInCorpus: Int, totalDocs: Int): Double = {
    val tf = termFrequencyInDoc.toDouble / totalTermsInDoc
    val docFreq = totalDocs.toDouble / termFreqInCorpus
    val idf = math.log(docFreq)
    tf * idf
  }

  /* TF-IDF captures two intuitions about the relevance of a term to a document. First, we would expect that
  * the more often a term occurs in a document, the more important it is to that document. Second, not all terms
  * are equal in a global sense. It is more meaningful to encounter a word that occurs rarely in the entire corpus
  * than a word that appears in most of the documents, thus the metric uses the inverse of the word’s appearance in
  * documents in the full corpus.
  *
  * The model relies on a few assumptions. It treats each document as a “bag of words,” meaning that it pays no
  * attention to the ordering of words, sentence structure, or negations. By representing each term once, the model
  * has difficulty dealing with polysemy, the use of the same word for multiple meanings. For example, the model
  * can’t distinguish between the use of “band” in “Radiohead is the best band ever” and “I broke a rubber band.”
  * If both sentences appear often in the corpus, it may come to associate “Radiohead” with “rubber.”
  *
  * The corpus has 10 million documents. Counting obscure technical jargon, the English language contains about
  * a million terms, some subset in the tens of thousands of which is likely useful for understanding the corpus.
  * Because the corpus contains far more documents than terms, it makes the most sense to generate the document-term
  * matrix as a row matrix—a collection of sparse vectors—each corresponding to a document
  * */

  /********************************************************************************************************************
    * **************************************** 14.3 Getting the Data ************************************************
    * ***************************************************************************************************************/

  /************************************* 14.3.1 Data preprocessing steps *********************************/
/* Getting from the raw Wikipedia dump into document-term matrix requires a set of preprocessing steps.
* - First, the input consists of a single enormous XML file with documents delimited by <page> tags. This needs
*   to be broken up to feed to the next step.
* - Second turning Wiki-formatting into plain text. The plain text is then split into tokens
* - Third tokens are reduced from their different inflectional forms to a root term through a process
*   called lemmatization
* - 4th These tokens can then be used to compute term and document frequencies.
* - 5th final step ties these frequencies together and builds the actual vector objects. In the book repo, all the
*    code for performing these steps is encapsulated in the AssembleDocumentTermMatrix class.
*
* The first steps can be performed for each document fully in parallel (which, in Spark, means as a set of map
* functions), but computing the inverse document frequencies requires aggregation across all the documents. A number
* of useful general NLP and Wikipedia-specific extraction tools exist that can aid in these tasks.
 */

/* This Lesson use some external dependencies, you need to add the following maven dependencies into your project
* pom file */

  def main(args:Array[String]):Unit={
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  val spark=SparkSession.builder().appName("Lesson14_Latent_Semantic_Analysis").master("local[2]").getOrCreate()
  import spark.implicits._

  val sparkConfig = ConfigFactory.load("application.conf").getConfig("spark")
  val  path= sparkConfig.getString("sourceDataPath")
  val filePath=s"${path}/spark_lessons/Lesson14_Latent_Semantic_Analysis/wikidump.xml"



  /************************************* 14.3.1.1 Transform xml to plain text *********************************/
  /* Here we use spark-xml lib to parse xml to dataSet, for more example, please check Lesson14_Spark_Xml_Parsing */

  val allPages=spark.read.format("com.databricks.spark.xml").option("rowTag","page").load(filePath)

  /* We only need two columns wiki page title and text*/

  val rawText=allPages.withColumn("text",$"revision.text._VALUE").select("title","text")

  //rawText.show(1,false)

  /************************************* 14.3.1.2  Lemmatization *********************************/

  /* With the plain text in hand, next we need to turn it into a bag of terms. This step requires care for a
  * couple of reasons. First, common words like “the” and “is” take up space but at best offer no useful information
  * to the model. Filtering out a list of stop words can both save space and improve fidelity. Second, terms
  * with the same meaning can often take slightly different forms. For example, “monkey” and “monkeys” do not
  * deserve to be separate terms. Nor do “nationalize” and “nationalization.” Combining these different inflectional
  * forms into single terms is called stemming or lemmatization. Stemming refers to heuristics-based techniques
  * for chopping off characters at the ends of words, while lemmatization refers to more principled approaches.
  * For example, the former might truncate “drew” to “dr,” while the latter might more correctly output “draw.”
  * The Stanford Core NLP project provides an excellent lemmatizer with a Java API that Scala can take advantage of.
  * */

  /* Build a library of not useful word, and broadcast to all nodes, (one copy per jvm instead of one copy per task).
  * We abandoned the below solution, because spark provide now a StopWordsRemover transformer now*/

  /*val stopWordPath="/DATA/data_set/spark/basics/Lesson14_Latent_Semantic_Analysis/stopWords.txt"
  val stopWords = scala.io.Source.fromFile(stopWordPath).getLines().toSet
  val bStopWords = spark.sparkContext.broadcast(stopWords)*/


  /* The terms contains two column : 1st column is the page title, 2nd column is a list of terms extract from the page
  * content by removing the stopWords*/

  val terms=textToTerms(rawText)

  /*terms.show(1,false)
  val count=terms.count()

  println(s"terms count : ${count} ")*/

  /* 167 row in all wiki geometry pages */

  /************************************* 14.3.1.3 Computing the TF-IDFs **************************************/

  /* At this point, terms refers to a data set of sequences of terms, each corresponding to a document. The next step
  * is to compute the frequencies for each term within each document and for each term within the entire corpus.
  * The spark ML lib contains Estimator and Transformer implementations for doing exactly this.
  * */

  /* we rename the column with title and terms and remove all rows which terms size = 1, in our case, it removes
  * nothing */
  val termsDF= terms.toDF("title","terms")
  val beforeFilteredCount=terms.count()
  val filteredTerms=termsDF.filter(size($"terms")>1)
  val afterFilteredCount=filteredTerms.count()

 /* println(s"Before filter terms has ${beforeFilteredCount}, after filter terms has ${afterFilteredCount}")
  filteredTerms.show(1,false)*/

  val numTerms = 20000
  /* CountVectorizer is an Estimator that can help compute the term frequencies for us. The CountVectorizer scans the
  * data to build up a vocabulary, a mapping of integers to terms, encapsulated in the CountVectorizerModel, a
  * Transformer. The CountVectorizerModel can then be used to generate a term frequency Vector for each document.
  * The vector has a component for each term in the vocabulary, and the value for each component is the number of times
  * the term appears in the document. Spark uses sparse vectors here, because documents typically only contain a small
  * sub-set of the full vocabulary.*/
  val countVectorizer = new CountVectorizer().
    setInputCol("terms").setOutputCol("termFreqs").
    setVocabSize(numTerms)
  val vocabModel = countVectorizer.fit(filteredTerms)
  val docTermFreqs = vocabModel.transform(filteredTerms)

  /* Notice the use of setVocabSize. The corpus contains millions of terms, but many are highly specialized words that
  * only appear in one or two documents. Filtering out less frequent terms can both improve performance and remove
  * noise. When we set a vocabulary size on the estimator, it leaves out all but the most frequent words.
   *
   * */
  docTermFreqs.cache()

  docTermFreqs.show(1)

  /* Now we have term frequency in each document, we need to calculate inverse document frequencies. IDF is another
  * Estimator, which counts the number of the documents in which each term in the corpus appears and then uses these
  * counts to compute the IDF scaling factor for each term. The IDFModel that it yields can then apply these scaling
  * factors to each term in each vector in the data set.*/

  val idf=new IDF().setInputCol("termFreqs").setOutputCol("tfidfVec")
  val idfModel=idf.fit(docTermFreqs)
  val docTermMatrix = idfModel.transform(docTermFreqs).select("title","tfidfVec","termFreqs")

  //docTermMatrix.show(1)

  /********************************* 14.3.1.4 Save the vec position and term string mapping **************************/
  /* As we descend from data frames into the world of vectors and matrices, we lose the ability to key by strings.
  * Thus, if we want to trace what we learn back to recognizable entities, it's important for us to save a mapping
  * of positions in the matrix to the terms and document titles in our original corpus. Positions in the term vectors
  * are equivalent to columns in our document-term matrix. The mapping of these positions to term strings is already
   * saved in our CountVectorizerModel. We can access it with: */

  val termIds:Array[String]=vocabModel.vocabulary

 // println(s" termIds : ${termIds.mkString(";")}")

  /* Creating a mapping of row IDs to document titles is a little more difficult. To achieve it, we can use the
  * zipWithUniqueId function, which associates a unique deterministic ID with every row in the DataFrame. We rely
  * on the fact that, if we call this function on a transformed version of the DataFrame, it will assign the same
  * unique IDs to the transformed rows as long as the transformations don't change the number of rows or their
  * partitioning. Thus, we can trace the rows back to their IDs in the DataFrame and, consequently, the document
  * titles that they correspond to:*/

  val docIds=docTermFreqs.rdd
    //map transform (title,terms,termFreqs) to title
    .map(_.getString(0))
    // zip transform title to (title,ID)
    .zipWithUniqueId()
    // swap transform (title,ID) to (ID,title)
    .map(_.swap)
    // toMap transform tuple (ID,title) to a Map(ID->title)
    .collect().toMap

 // println(s"docIds ${docIds.toString()}")

  /************************************* 14.3.2 Singular Value Decomposition  *********************************/

  /* With the document-term matrix M in hand, the analysis can proceed to the factorization and dimensionality
  * reduction. Spark ML contains an implementation of the SVD that can handle enormous matrices. The singular value
  * decomposition takes an m*n matrix and returns three matrices that approximately equal it when multiplied together:
  * M ~= USV (P123 has more details)
  *
  * The matrices are :
  * - U is an m*k matrix whose columns form an orthonormal basis for the document space. m is the number of doc
  * - S is a k*k diagonal matrix, each of whose entries correspond to the strength of one the concepts, k is the
  *           number of concept.
  * - V is a k*n matrix whose columns form an orthonormal basis for the term space. n is the number of term
  *
  * In the LSA case, m is the number of documents and n is the number of terms. The decomposition is parameterized with
  * a number k, less than or equal to n, which indicates how many concepts to keep around. When k=n, the product of the
  * factor matrices reconstitutes the original matrix exactly. When k<n, the multiplication results in a low-rank
  * approximation of the original matrix. k is typically chosen to be much smaller than n. SVD ensures that the
  * approximation will be the closest possible to the original matrix.
  *
  * */

  /* At the time of writing this lesson, there is no SVD implementation in spark.ml, but spark.mllib has a SVD
  * implementation which only operates on RDD. As a result, we need to transform our dataframe to mllib vectors based
  * on RDDs*/

  val vecRdd= docTermMatrix.select("tfidfVec").rdd
    .map{row=>Vectors.fromML(row.getAs[MLVector]("tfidfVec"))
    }

  /* To find the singular value decomposition, we simply wrap an RDD of row vectors in a RowMatrix and call computeSVD */

  vecRdd.cache()
  val mat=new RowMatrix(vecRdd)
  val k =1000
  val svd=mat.computeSVD(k,computeU = true)

  /* The RDD should be cached in memory beforehand because the computation requires multiple passes over the data.
  * The computation requires O(nk) storage on the driver, O(n) storage for each task, and O(k) passes over the data.
  * */


  /************************************* 14.3.3 Finding Important Concepts  *********************************/

  /* SVD outputs a bunch of numbers. How can we inspect these to verify they actually relate to anything useful?
  * The V matrix represents concepts through the terms that are important to them. As discussed earlier, V contains
  * a column for every concept and a row for every term. The value at each position can be interpreted as the
  * relevance of that term to that concept. This means that the most relevant terms to each of the top concepts can
  * be found with something like this
  * */
  val topConceptTerms = topTermsInTopConcepts(svd, 4, 10, termIds)
  val topConceptDocs = topDocsInTopConcepts(svd, 4, 10, docIds)
  for ((terms, docs) <- topConceptTerms.zip(topConceptDocs)) {
    println("Concept terms: " + terms.map(_._1).mkString(", "))
    println("Concept docs: " + docs.map(_._1).mkString(", "))
    println()
  }
/* The output are following

Concept terms: sub, math, x, anatomy, d, phi_t, y, geodesic, 1, h
Concept docs: Computational anatomy, Busemann function, Large deformation diffeomorphic metric mapping, Minkowski space,
              Complex reflection group, Riemannian metric and Lie bracket in computational anatomy, Diffeomorphometry,
              Bayesian model of computational anatomy, Snub (geometry), Huzita–Hatori axioms

Concept terms: sub, y, x, h, d, sup, hadamard, gamma, busemann, harvnb
Concept docs: Busemann function, Complex reflection group, Huzita–Hatori axioms, Line moiré, Snub (geometry),
              Minkowski space, Symmetry group, Volume and displacement indicators for an architectural structure,
              Geometric separator, Infinitely near point

Concept terms: node_h, snub, cdd, bmatrix, node, br, uniform, sr, tiling, 60px
Concept docs: Snub (geometry), Complex reflection group, Minkowski space, Finite subdivision rule,
              Schema for horizontal dials, Geometry processing, Strähle construction, Minkowski plane,
              Coxeter decompositions of hyperbolic polygons, Laguerre plane

Concept terms: minkowski, u, mathbf, spacetime, sup, math, vectors, mvar, relativity, left
Concept docs: Minkowski space, Minkowski diagram, Minkowski plane, Fat object, Complex reflection group,
              Base change theorems, Space, Superspace, Surface (mathematics), Geometry processing

We could say the grouping of terms and docs is pretty impressive. Even though unexpected words appear in each, all
the concepts exhibit some thematic coherence.
*/

  /***************************************************************************************************************
    ******************* 14.4 Querying and Scoring with a Low-Dimensional Representation  ********************
    * *********************************************************************************************************/

  /* How relevant is a term to a document? How relevant are two terms to each other? Which documents most closely
  * match a set of query terms? The original document-term matrix provides a shallow way to answer these qutions.
  * We can achieve a relevance score between two terms by computing the cosine similarity between their two column
  * vectors in the matrix. Cosine similarity measures the angle between two vectors. Vectors that point in the same
  * direction in the high-dimensional document space are thought to be relevant to each other. This is computed as the
  * dot product of the vectors divided by the product of their lengths.
  *
  * Cosine similarity sees wide use as a similarity metric between vectors of term and document weights in natural
  * language and information retrieval applications. Likewise, for two documents, a relevance score can be computed
  * as the cosine similarity between their two row vectors. A relevance score between a term and document can simply
  * be the element in the matrix at the intersection of both.
  *
  * However, these scores come from shallow knowledge about the relationships between these entities, relying on simple
  * frequency counts. LSA provides the ability to base these scores on a deeper understanding of the corpus. For example,
  * if the term "artilery" appears nowhere in a document on the "Normandy landing" article, but it mentions "howitzer"
  * frequently, the LSA representation may be able to recover the relation between "artillery" and the article based
  * on the co-occurrence of "artillery" and "howitzer" in other documents
  *
  * The LSA representation also offers benefits from an efficency standpoint. It packs the important information into
  * a lower-dimensional representation that can be operated on instead of the original document-term matrix. Consider
  * the task of finding the set of terms most relevant to a particular term. The naive approach requires computing
  * the dot product between that term's column vector and every other column vector in the document-term matrix.
  * This involves a number of multiplications proportional to the number of terms times the number of document.
  * LSA can achieve the same by looking up its concept-space representation and mapping it back into term space,
  * requiring a number of multiplication encodes the relevant patterns in the data, so the full corpus need not
  * be queried.
  *
  * In the following section, we'll build a primitive query engine using the LSA representation of our data.
  * */

  /*********************************************** 14.4.1 Term-Term relevance ************************************/

  /* LSA understands the relation between two terms as the cosine similarity between their tow columns in the
  * reconstructed low-rank matrix; that is, the matrix that would be produced if the three approximate factors were
  * multiplied back together. One of the ideas behind LSA is that this matrix offers a more useful representation of
  * the data. It offers this in a few ways:
  * - accounting for synonymy by condensing related terms
  * - accounting for polysemy by placing less weight on terms that have multiple meanings
  * - Throwing out noise
  *
  * However, we do not need actually calculate the contents of this matrix to discover the cosine similarity. Some
  * linear algebra manipulation reveals that the cosine similarity between two columns in the reconstructed matrix
  * is exactly equal to the cosine similarity between the corresponding columns in SV. Consider the task of finding
  * the set of terms most relevant to a particular term. Finding the cosine similarity between a term and all other
  * terms is equivalent to normalizing each row in VS to length 1 and then multiplying the row corresponding to that
  * term by it. Each element in the resulting vector will contain a similarity between a term and the query term.
  * */

  /*P 131 To be continued*/
}







  def textToTerms(data:DataFrame):DataFrame={

    /* Build a regex tokenizer which only takes words into account */
    val regexTokenizer=new RegexTokenizer()
      .setInputCol("text")
      .setOutputCol("rawTerms")
      .setPattern("\\W")

    /* transform the data */
    val tokenizedData=regexTokenizer.transform(data)

    /* build a stop word remover*/
    val stopWordRemover=new StopWordsRemover()
      .setInputCol("rawTerms")
      .setOutputCol("terms")

    val filteredTerms=stopWordRemover.transform(tokenizedData)

    filteredTerms.select("title","terms")
  }


  /**********************************  14.3.3 Finding Important Concepts  *******************************/
  /*
  * This method finds the topTerms for each top Concept*/
  def topTermsInTopConcepts(svd:SingularValueDecomposition[RowMatrix,Matrix],numConcepts:Int,numTerms:Int,
                            termIds:Array[String]):Seq[Seq[(String,Double)]]={

    /* V is a k*n matrix, k is the number of concept, n is the number of terms by using matrix V*/
    val v=svd.V
    val topTerms= new ArrayBuffer[Seq[(String, Double)]]()
    // When we convert v to array, which is not a distributed data type, it will be stored in the driver process only
    val arr = v.toArray
    for (i <- 0 until numConcepts) {
      val offs = i * v.numRows
      val termWeights = arr.slice(offs, offs + v.numRows).zipWithIndex
      val sorted = termWeights.sortBy(-_._1)
      topTerms += sorted.take(numTerms).map {
        /* This last step finds the actual terms that correspond to the positions in the term vectors. Recall
        * that termIds is the integer -> term mapping we got from the CountVectorizer*/
        case (score, id) => (termIds(id), score)
      }
    }

    topTerms

    /* Note that V is a matrix in local memory in the driver process, and the computation occurs in a non-distributed
    * manner.*/
  }

  /* This method finds the documents relevant to each of the top concepts by using matrix U */

  def topDocsInTopConcepts(svd: SingularValueDecomposition[RowMatrix, Matrix], numConcepts: Int, numDocs: Int,
                           docIds: Map[Long, String]): Seq[Seq[(String, Double)]] = {
    val u = svd.U
    println(s"u has type ${u.getClass.getName}")
    val topDocs = new ArrayBuffer[Seq[(String, Double)]]()
    for (i <- 0 until numConcepts) {
      /* monotonically_increasing_id/zipWithUniqueId trick discussed in the previous section of this Lesson. This
      * allows us to maintain continuity between rows in the matrix and rows in the DataFrame it is derived from,
      * which also has the titles.*/
      val docWeights = u.rows.map(_.toArray(i)).zipWithUniqueId()
      topDocs += docWeights.top(numDocs).map {
        case (score, id) => (docIds(id), score)
      }
    }
    topDocs
    /* Note that, in this code, U is stored as a distributed matrix.*/
  }






  /* This method takes a String of format xml (represents a page wiki), it returns a tuple (title:String,content:String)
   *  */


  /************************************************************Appendix *********************************************/

/* We tried to used code in the book Advanced Analytics with spark, which needs the following maven dependencies. But
we have encontered many problems, so we use our own xml parsing and tokenizer code. Our code only requires databricks
spark-xml lib to work.

<!-- Dependencies for nlp (Lesson14) -->
        <dependency>
            <groupId>edu.stanford.nlp</groupId>
            <artifactId>stanford-corenlp</artifactId>
            <version>${corenlp.version}</version>
        </dependency>
        <dependency>
            <groupId>edu.stanford.nlp</groupId>
            <artifactId>stanford-corenlp</artifactId>
            <version>${corenlp.version}</version>
            <classifier>models</classifier>
        </dependency>
        <dependency>
            <groupId>edu.umd</groupId>
            <artifactId>cloud9</artifactId>
            <version>2.0.1</version>
        </dependency>
        <dependency>
            <groupId>info.bliki.wiki</groupId>
            <artifactId>bliki-core</artifactId>
            <version>3.1.0</version>
        </dependency>
        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-client</artifactId>
        </dependency>
        */


  /* There is a bug in the edu.umd.cloud9.collection.XMLInputFormat$XMLRecordReader.nextKeyValue which throws a
   * java.lang.RuntimeException: bytes consumed error! */
}
