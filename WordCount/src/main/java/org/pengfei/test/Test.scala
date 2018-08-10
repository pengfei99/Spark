package org.pengfei.test

import org.pengfei.spark.formation.TweetsStat.lineWordCount

object Test {

  def main(args:Array[String]): Unit ={
    val test="I'm pengfei liu"
    print(lineWordCount(test))
  }

  def lineWordCount(text: String): Long={

    val word=text.split(" ").map(_.toLowerCase).groupBy(identity).mapValues(_.size)
    val counts=word.foldLeft(0){case (a,(k,v))=>a+v}
   /* print(word)
    print(counts)*/
    return counts

  }
}
