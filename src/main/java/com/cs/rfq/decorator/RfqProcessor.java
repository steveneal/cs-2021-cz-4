package com.cs.rfq.decorator;

import com.cs.rfq.decorator.extractors.RfqMetadataExtractor;
import com.cs.rfq.decorator.extractors.RfqMetadataFieldNames;
import com.cs.rfq.decorator.extractors.TotalTradesWithEntityExtractor;
import com.cs.rfq.decorator.extractors.VolumeTradedWithEntityYTDExtractor;
import com.cs.rfq.decorator.publishers.MetadataJsonLogPublisher;
import com.cs.rfq.decorator.publishers.MetadataPublisher;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.apache.spark.sql.functions.sum;

public class RfqProcessor {

    private final static Logger log = LoggerFactory.getLogger(RfqProcessor.class);

    private final SparkSession session;

    private final JavaStreamingContext streamingContext;

    private Dataset<Row> trades;

    private final List<RfqMetadataExtractor> extractors = new ArrayList<>();

    private final MetadataPublisher publisher = new MetadataJsonLogPublisher();

    private  Map instrumentMap;


    public RfqProcessor(SparkSession session, JavaStreamingContext streamingContext) {
        this.session = session;
        this.streamingContext = streamingContext;

        //TODO: use the TradeDataLoader to load the trade data archives

        //TODO: take a close look at how these two extractors are implemented
        extractors.add(new TotalTradesWithEntityExtractor());
        extractors.add(new VolumeTradedWithEntityYTDExtractor());
    }
    public static void main(String[] args) throws Exception {

        startSocketListener();
    }
    public static void startSocketListener() throws InterruptedException {
        //TODO: stream data from the input socket on localhost:9000
        System.setProperty("hadoop.home.dir", "C:\\Java\\hadoop-2.9.2");
        System.setProperty("spark.master", "local[4]");

        SparkConf conf = new SparkConf().setAppName("StreamFromSocket");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.milliseconds(100));

        JavaDStream<String> lines = jssc.socketTextStream("localhost", 9000);
        //JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(x.split(" ")).iterator());

        //TODO: convert each incoming line to a Rfq object and call processRfq method with it
        lines.foreachRDD(rdd -> {
            rdd.collect().forEach(line -> processRfq(Rfq.fromJson(line.toString())));
        });


        //TODO: start the streaming context
        jssc.start();
        jssc.awaitTermination();
    }

    public static void processRfq(Rfq rfq) {
        log.info(String.format("Received Rfq: %s", rfq.toString()));

        //create a blank map for the metadata to be collected
        Map<RfqMetadataFieldNames, Object> metadata = new HashMap<>();
        //String liquidty = metadata.lookupLiquidity(rfq.getIsin());
        //metadata.put(RfqMetadataFieldNames.liquidity, liquidty);

        //TODO: get metadata from each of the extractors
        System.out.println(rfq.getEntityId());
        //TODO: publish the metadata

    }
}
