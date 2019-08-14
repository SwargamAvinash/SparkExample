package com.swargam.example.sparkexample;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class WordCount {

    public static void main(String[] args){
        if(args.length == 0) {
            System.out.println("Wrong format of input ::");
            System.exit(0);
        }
        wordCountJava8(args[0]);
    }

    private static void wordCountJava8(String filename) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Work count App");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> input  = sc.textFile(filename);
        JavaRDD<String> words = input.flatMap(s-> Arrays.asList(s.split(" ")));
        JavaPairRDD<String,Integer> counts = words.mapToPair(t-> new Tuple2(t,1)).reduceByKey((x, y)->(int)x + (int)y);

        counts.saveAsTextFile("output_spark_counts");
    }
}
