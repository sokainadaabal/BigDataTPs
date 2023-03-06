package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;

import java.util.Arrays;

public class Main {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf();

        conf.setAppName("TP 1 spark").setMaster("local[*]");
        SparkContext sc = new SparkContext(conf);
        JavaRDD<Double> root= sc.parallelize(Arrays.asList(12.5,16,8,2.9));

    }
}