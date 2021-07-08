package com.cs.rfq.utils;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.Arrays;

public class RfqServer extends Thread
{
    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", "C:\\Java\\hadoop-2.9.2");
        System.setProperty("spark.master", "local[4]");

        SparkConf conf = new SparkConf().setAppName("StreamFromSocket");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        JavaDStream<String> lines = jssc.socketTextStream("localhost", 9000);
        JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(x.split(" ")).iterator());

        //print out the results
        words.foreachRDD(rdd -> {
            rdd.collect().forEach(line -> consume(line));
        });

        jssc.start();
        jssc.awaitTermination();
    }

    static void consume(String line) {
        System.out.println(line);
    }
}
