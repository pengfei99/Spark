package org.pengfei.tmp

import java.util

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{BooleanType, IntegerType, StringType, StructField, StructType}
import org.pengfei.Lesson04_Spark_SQL.MergeListsUDAF

import scala.collection.mutable

object Test {

  def main(args:Array[String]):Unit={
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
   val spark=SparkSession.builder().appName("Test").master("local[2]").getOrCreate()
    import spark.implicits._

    val chantier_201_filePath="/home/pliu/Documents/Projects/Hyperthesaux/Sample_Data/Bibracte/Extraction_BDB/export_bdB_10_09_2019/201chantier.csv"
    val chantier_201_Schema = StructType(Array(
      StructField("annees_de_fonctionne", StringType, true),
      StructField("auteur_saisie", StringType, true),
      StructField("chantier", IntegerType, true),
      StructField("code_OA", IntegerType, true),
      StructField("commentaire", StringType, true),
      StructField("commentaire_chantier", StringType, true),
      StructField("compteur", StringType, true),
      StructField("date_derniere_modif", StringType, true),
      StructField("date_saisie", StringType, true),
      StructField("lieu_dit_adresse", StringType, true),
      StructField("localisation_cadastral", IntegerType, true),
      StructField("nom_chantier", StringType, true),
      StructField("nom_commune", StringType, true),
      StructField("nom_departement", StringType, true),
      StructField("numero_INSEE_comm", IntegerType, true),
      StructField("proprietaire", StringType, true),
      StructField("proprietaire unique", StringType, true),
      StructField("tampon_1", StringType, true),
      StructField("total_fiche_trouvee", IntegerType, true),
      StructField("xcentroide", IntegerType, true),
      StructField("xmax", IntegerType, true),
      StructField("xmin", IntegerType, true),
      StructField("ycentroide", IntegerType, true),
      StructField("ymax", IntegerType, true),
      StructField("ymin", IntegerType, true)))


    val chantierDF = spark.read.format("com.databricks.spark.csv").option("header", "false").schema(chantier_201_Schema).load(chantier_201_filePath)

    chantierDF.show(5,false)

    val beuvrayDF=chantierDF.filter(col("nom_chantier").contains("Mont Beuvray"))

    beuvrayDF.show(15)

    chantierDF.select("auteur_saisie").distinct().show(10);

    val rapDf=chantierDF.filter(col("auteur_saisie")===("Raphaël Moreau"))

rapDf.show(5)

    val userEnterCount=chantierDF.groupBy("auteur_saisie").count().orderBy(col("count").desc).show()

    val countNull=chantierDF.select("nom_chantier").filter(col("nom_chantier").isNull).count()
    println
  }



  def myConcat(text1:String,text2:String):String={
    text1.concat(text2)
  }

  def combineTwoColumn(cluster:Int,label:String):(Int,String)={
    return (cluster,label)
  }

  def labelCount(label:String):(String,Int)={
    return (label,1)
  }

  def entropy(counts: Iterable[Int]):Double={
    // get all positive values in the counts collection
    val values=counts.filter(_ >0)
    // cast all values to double and do sum
    val n = values.map(_.toDouble).sum
    val entropy=values.map{v=>
      //calculate p first
      val p=v / n
      //
      -p * math.log(p)
    }.sum

    /* We can use the following code, if you don't want to define a local variable

    values.map { v =>
      -(v / n) * math.log(v / n)
    }.sum
    */

    return entropy
  }
}
