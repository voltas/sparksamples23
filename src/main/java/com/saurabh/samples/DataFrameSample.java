package com.saurabh.samples;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DataFrameSample {
    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .master("local[*]")
                .config("spark.sql.shuffle.partitions", 2)
                .getOrCreate();

        Dataset<Row> df = spark.read().json("src/main/resource/people.json");

        // using dataframe DSL
        df.show();

        // using sql query
        df.registerTempTable("sampleTable");
        spark.sql("select name from sampleTable").show();

    }
}
