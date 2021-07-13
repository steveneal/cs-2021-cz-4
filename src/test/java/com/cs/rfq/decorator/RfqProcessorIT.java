package com.cs.rfq.decorator;

import com.cs.rfq.decorator.extractors.AbstractSparkUnitTest;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RfqProcessorIT {

    private final static Logger log = LoggerFactory.getLogger(AbstractSparkUnitTest.class);

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
        log.info("Spark configuration complete");

        //TODO: create a Spark streaming context
        jssc = new JavaStreamingContext(conf, Durations.milliseconds(100));
        log.info("Spark Streaming Context complete");

        //TODO: create a Spark session
        session = SparkSession.builder().config(conf).getOrCreate();
        //TODO: create a new RfqProcessor and set it listening for incoming RFQs
        log.info("Spark Session complete");

        TradeDataLoader trades = new TradeDataLoader();
        trades_df = trades.loadTrades(session, "src/test/resources/trades/trades.json");
    }

    @Test
    public void testKafkaCommunication() throws Exception {

        Producer producer = new Producer("127.0.0.1:9092");
        RfqProcessor rfqProcessor = new RfqProcessor(session, jssc);
        Rfq testMessage = new Rfq();
        testMessage.setId("End-to-End message");
        rfqProcessor.processRfq(testMessage, session, trades_df);
        log.info("Test Kafka Communication");
    }
}
