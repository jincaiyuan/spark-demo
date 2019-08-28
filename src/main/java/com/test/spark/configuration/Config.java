package com.test.spark.configuration;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class Config {

    private static final String MASTER = "spark://192.168.234.128:7077";
    private static final String JAR = "E:\\ideaProject\\spark-test\\target\\spark-test-1.0-SNAPSHOT.jar";

//    @Bean
//    public SparkSession javaSparkSession() {
//        SparkConf conf = new SparkConf().setMaster(MASTER)
//                .setAppName("word count");
//        conf.setJars(new String[] {JAR});
//        SparkSession session = SparkSession.builder().config(conf).getOrCreate();
//        return session;
//    }

    @Bean
    public JavaSparkContext javaSparkContext() {
        SparkConf conf = new SparkConf().setMaster(MASTER).setAppName("demo").
                setJars(new String[]{JAR});
        JavaSparkContext context = new JavaSparkContext(conf);
        return context;
    }
}
