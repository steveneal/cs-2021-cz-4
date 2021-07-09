package com.cs.rfq.decorator;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class RfqDecoratorMain {

    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", "C:\\Java\\hadoop-2.9.2");
        System.setProperty("spark.master", "local[4]");

        //TODO: create a Spark configuration and set a sensible app name
        SparkConf conf = new SparkConf().setAppName("RFQDecorator");
        //TODO: create a Spark streaming context
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.milliseconds(100));
        //TODO: create a Spark session
        SparkSession session = SparkSession.builder().config(conf).getOrCreate();
        //TODO: create a new RfqProcessor and set it listening for incoming RFQs
        RfqProcessor rfqproc = new RfqProcessor(session, jssc);
    }
}
