package com.sparkstart;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;


public class SparkSessionApplication {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession
                .builder()
                .master("local")
                .appName("SQL query")
                .getOrCreate();
        Dataset<Row> json = sparkSession.read().json("src/main/resources/data.txt");
        json.show();
        json.printSchema();
        json.select(json.col("name")).show();
        json.select(json.col("name"), json.col("age")).show();
        json.filter(json.col("age").gt(25)).show();
        json.groupBy("name").count().show();
        // json to bd in memory
        json.createOrReplaceTempView("people");
        Dataset<Row> sql = sparkSession.sql("select * from people");
        sql.show();

        // read txt scf format table data
        JavaRDD<Person> personJavaRDD = sparkSession.read().textFile("src/main/resources/name.txt")
                .javaRDD()
                .map(line -> {
                    String[] parts = line.split(",");
                    Person person = new Person();
                    person.setName(parts[0]);
                    person.setAge(Integer.parseInt(parts[1].trim()));
                    return person;
                });
        System.out.println(personJavaRDD.collect());
    }

}
