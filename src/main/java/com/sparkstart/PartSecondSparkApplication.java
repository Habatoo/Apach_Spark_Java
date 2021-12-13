package com.sparkstart;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.LongAccumulator;

import java.util.Arrays;

public class PartSecondSparkApplication {
    public static void main(String[] args) {
        // Read configuration
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("SparkStart");
        // Create context
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        // Create RDD object from context from out context
        JavaRDD<String> stringJavaRDD = javaSparkContext.textFile("src/main/resources/data.txt");
        Broadcast<JavaRDD<String>> broadcast = javaSparkContext.broadcast(stringJavaRDD);
        broadcast.value();
        // accumulator
        LongAccumulator longAccumulator = javaSparkContext.sc().longAccumulator();
        javaSparkContext.parallelize(
                Arrays.asList(1, 2, 3, 4)).map(x -> {longAccumulator.add(x); return x * 2;}).collect();
        System.out.println(longAccumulator.value());

    }


}
