package com.sparkstart;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SparkApplication {
    public static void main(String[] args) {
        // Read configuration
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("SparkStart");
        // Create context
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        // Create RDD object from context from out context
        JavaRDD<String> stringJavaRDD = javaSparkContext.textFile("src/main/resources/data.txt");
        System.out.println(stringJavaRDD.count());

        // Create RDD object from memory
        JavaRDD<Integer> integerJavaRDD = javaSparkContext.parallelize(Arrays.asList(1, 2, 3));
        // Transformation - lazy - create other object RDD
        // map, filter, flatMap, distinct, union, intersection, subtract
        JavaRDD<Integer> map = integerJavaRDD.map(x -> x * 2);
        map.foreach(x -> System.out.println(x));
        JavaRDD<Integer> filter = map.filter(x -> x != 2);
        filter.foreach(x -> System.out.println(x));
        JavaRDD<Integer> flatMap = filter.flatMap(x -> new ArrayList(Arrays.asList(x, x * 2)).iterator());
        flatMap.foreach(x -> System.out.println(x));
        JavaRDD<Integer> integerJavaRDD_doubles = javaSparkContext.parallelize(Arrays.asList(1, 1, 2, 3));
        integerJavaRDD_doubles.distinct().foreach(x -> System.out.println(x));
        integerJavaRDD.distinct().foreach(x -> System.out.println(x));
        integerJavaRDD.union(integerJavaRDD_doubles).foreach(x -> System.out.println(x));
        integerJavaRDD.intersection(integerJavaRDD_doubles).foreach(x -> System.out.println(x));
        integerJavaRDD_doubles.subtract(integerJavaRDD).foreach(x -> System.out.println(x));
        // Action
        // reduce, collect, take, top, takeSample, count, countByValue
        JavaRDD<Integer> integerJavaRDD_long = javaSparkContext.parallelize(Arrays.asList(1, 2, 3, 4, 4));
        System.out.println(integerJavaRDD_long.reduce((x, y) -> x + y));
        System.out.println(integerJavaRDD_long.reduce((x, y) -> x * y));
        System.out.println(integerJavaRDD_long.collect()); // to collection object
        System.out.println(integerJavaRDD_long.count()); // size
        System.out.println(integerJavaRDD_long.countByValue()); // number of value
        System.out.println(integerJavaRDD_long.take(3)); // head object
        System.out.println(integerJavaRDD_long.top(2)); // tail object
        System.out.println(integerJavaRDD_long.takeSample(true,2)); // sample of object, number = 2

        // Cash - for several operations on one RDD object
        JavaRDD<Integer> integerJavaRDD_cash = javaSparkContext.parallelize(Arrays.asList(1, 2, 3, 4, 10, 12));
        integerJavaRDD_cash.persist(StorageLevel.DISK_ONLY()); // save data on disc
        // integerJavaRDD_cash.persist(StorageLevel.MEMORY_ONLY()); // in memory
        integerJavaRDD_cash.count();
        integerJavaRDD_cash.collect();
        integerJavaRDD_cash.unpersist(); // scip cash

        // Pair RDD
        Tuple2<String, String> tuple2 = new Tuple2<>("first", "second");
        System.out.println("Pair RDD");
        System.out.println(tuple2._1());
        System.out.println(tuple2._2());

        JavaRDD<String> lines = javaSparkContext.parallelize(Arrays.asList("first", "second", "third"));
        JavaPairRDD<String, String> pairRDD = lines.mapToPair(x -> new Tuple2<>(x, x));
        System.out.println(pairRDD.collect());

        List<Tuple2<String, String>> list = Arrays.asList(new Tuple2<>("1", "first"), new Tuple2<>("2", "second"));
        JavaPairRDD<String, String> newPairRDD = javaSparkContext.parallelizePairs(list);
        JavaPairRDD<String, String> newFilter = newPairRDD.filter(x -> x._1().equals("1"));
        System.out.println(newFilter.collect());

        List<Tuple2<String, String>> list_3 = Arrays.asList(
                new Tuple2<>("1", "first"),
                new Tuple2<>("1", "new"),
                new Tuple2<>("2", "second"));
        JavaPairRDD<String, String> newPairRDD_3 = javaSparkContext.parallelizePairs(list_3);
        JavaPairRDD<String, String> newFilter_3 = newPairRDD_3.reduceByKey((x, y) -> x + y);
        System.out.println(newFilter_3.collect());

        JavaPairRDD<String, Iterable<String>> groupByKey = newPairRDD_3.groupByKey();
        System.out.println(groupByKey.collect());

        // 2 Pair RDD
        List<Tuple2<String, String>> list_4 = Arrays.asList(
                new Tuple2<>("1", "first"),
                new Tuple2<>("2", "second"));
        List<Tuple2<String, String>> list_5 = Arrays.asList(
                new Tuple2<>("2", "two"),
                new Tuple2<>("2", "three"));
        JavaPairRDD<String, String> newPairRDD_4 = javaSparkContext.parallelizePairs(list_4);
        JavaPairRDD<String, String> newPairRDD_5 = javaSparkContext.parallelizePairs(list_5);
        JavaPairRDD<String, Tuple2<String, String>> joinPairRDD = newPairRDD_4.join(newPairRDD_5);
        System.out.println(joinPairRDD.collect());

        // sort Pair RDD
        List<Tuple2<String, String>> list_6 = Arrays.asList(
                new Tuple2<>("c", "first"),
                new Tuple2<>("a", "first"),
                new Tuple2<>("b", "second"));
        JavaPairRDD<String, String> newPairRDD_6 = javaSparkContext.parallelizePairs(list_6);
        JavaPairRDD<String, String> sortPairRDD = newPairRDD_6.sortByKey();
        System.out.println(sortPairRDD.collect());

        // read data
        JavaRDD<String> textFile = javaSparkContext.textFile("src/main/resources/data.txt");
        JavaPairRDD<String, String> pairRDD1 = javaSparkContext.wholeTextFiles("src/main/resources/data.txt");
        System.out.println(textFile);
        System.out.println(pairRDD1);
        textFile.saveAsTextFile("src/main/resources/text");
        // save as object
        textFile.saveAsObjectFile("src/main/resources/object");
        // save in hadoop format
        JavaPairRDD<Text, IntWritable> javaPairRDD = textFile.mapToPair(
                record -> new Tuple2<>(new Text(record), new IntWritable()));
        javaPairRDD.saveAsHadoopFile(
                "src/main/resources/hadoop", Text.class, IntWritable.class, SequenceFileOutputFormat.class);
        // read as hadoop
        JavaPairRDD<Text, IntWritable> textIntWritableJavaPairRDD = javaSparkContext.sequenceFile(
                "src/main/resources/hadoop", Text.class, IntWritable.class);
        textIntWritableJavaPairRDD.map(f -> f._1().toString()).foreach(x -> System.out.println(x));
    }
}
