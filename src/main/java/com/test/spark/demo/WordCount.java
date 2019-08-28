//package com.test.spark.demo;
//
//import org.apache.spark.api.java.JavaPairRDD;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.sql.SparkSession;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.stereotype.Component;
//import scala.Tuple2;
//
//import javax.annotation.PostConstruct;
//import java.io.Serializable;
//import java.util.Arrays;
//import java.util.List;
//import java.util.regex.Pattern;
//
//@Component
//public class WordCount implements Serializable {
//
//    @Autowired
//    private transient SparkSession session;
//
//
//    private static final Pattern SPACE = Pattern.compile(" ");
//
//    public void wordCount() {
//        JavaRDD<String> input = session.read().textFile("file:///E:/ideaProject/spark-test/src/main/resources/README.md").javaRDD();
//        JavaRDD<String> words = input.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator());
//
//        JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));
//
//        JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);
//        List<Tuple2<String, Integer>> output = counts.collect();
//        for (Tuple2<?,?> tuple : output) {
//            System.out.println(tuple._1() + ": " + tuple._2());
//        }
//
//        session.close();
//    }
//}
