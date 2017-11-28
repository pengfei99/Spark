package org.pengfei.spark.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class WordCount {

    public static void main(String[] args){

        SparkConf conf = new SparkConf().setMaster("local").setAppName("wordCount");
        JavaSparkContext sc= new JavaSparkContext(conf);

        String inputFile = "hdfs://localhost:9000/test_data/pengfei.txt";

        JavaRDD<String> input = sc.textFile(inputFile);

        // Split in to list of words
        JavaRDD < String > words = input.flatMap(l -> Arrays.asList( l.split(" ")).iterator());
        // Transform into pairs and count.
        JavaPairRDD< String, Integer > pairs = words.mapToPair(w -> new Tuple2(w, 1));

        JavaPairRDD < String, Integer > counts = pairs.reduceByKey((x, y) -> x + y);

        System.out.println(counts.collect());
    }
}
