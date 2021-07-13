package com.cs.rfq.decorator;

import com.cs.rfq.utils.Producer;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mock;

public class RfqProcessorIT {
    static SparkSession session;
    static SparkConf conf;
    static Dataset<Row> trades_df;
    static JavaStreamingContext jssc;

    @BeforeClass
    public static void setup() throws Exception {
        System.setProperty("hadoop.home.dir", "C:\\Java\\hadoop-2.9.2");
        System.setProperty("spark.master", "local[4]");

        //TODO: create a Spark configuration and set a sensible app name
        conf = new SparkConf().setAppName("RFQDecorator");
        //TODO: create a Spark streaming context
        jssc = new JavaStreamingContext(conf, Durations.milliseconds(100));
        //TODO: create a Spark session
        session = SparkSession.builder().config(conf).getOrCreate();
        //TODO: create a new RfqProcessor and set it listening for incoming RFQs
        TradeDataLoader trades = new TradeDataLoader();
        trades_df = trades.loadTrades(session, "src/test/resources/trades/trades.json");
    }

    @Test
    public void testKafkaCommunication() throws Exception {
        Producer producer = new Producer("127.0.0.1:9092");
        RfqProcessor rfqProcessor = new RfqProcessor(session, jssc);
        rfqProcessor.processRfq(new Rfq(), session, trades_df);
    }

}
