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

//    public final SparkSession sessions
//
//    private final JavaStreamingContext streamingContext;

//    public final Dataset<Row> trades_df

    private final List<RfqMetadataExtractor> extractors = new ArrayList<>();

    private final MetadataPublisher publisher = new MetadataJsonLogPublisher();

    private  Map instrumentMap;


    public RfqProcessor(SparkSession sessions, JavaStreamingContext streamingContext) throws Exception{
//        this.session = session;
//        this.streamingContext = streamingContext;

        //TODO: use the TradeDataLoader to load the trade data archives
        TradeDataLoader trades = new TradeDataLoader();
        Dataset <Row> trades_df = trades.loadTrades(sessions, "src/test/resources/trades/trades.json");
        //TODO: take a close look at how these two extractors are implemented
        extractors.add(new TotalTradesWithEntityExtractor());
        extractors.add(new VolumeTradedWithEntityYTDExtractor());
        startSocketListener(sessions, streamingContext, trades_df);
    }

    public static void startSocketListener(SparkSession sessions, JavaStreamingContext jssc, Dataset<Row> trades_df) throws InterruptedException {
        //TODO: stream data from the input socket on localhost:9000

        JavaDStream<String> lines = jssc.socketTextStream("localhost", 9000);
        //JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(x.split(" ")).iterator());

        //TODO: convert each incoming line to a Rfq object and call processRfq method with it
        lines.foreachRDD(rdd -> {
            rdd.collect().forEach(line -> processRfq(Rfq.fromJson(line.toString()), sessions, trades_df));
        });


        //TODO: start the streaming context
        jssc.start();
        jssc.awaitTermination();
    }

    public static void processRfq(Rfq rfq, SparkSession sessions, Dataset<Row> trades_df) {
        log.info(String.format("Received Rfq: %s", rfq.toString()));

        //create a blank map for the metadata to be collected
        Map<RfqMetadataFieldNames, Object> metadata = new HashMap<>();
        //String liquidty = metadata.lookupLiquidity(rfq.getIsin());
        //metadata.put(RfqMetadataFieldNames.liquidity, liquidty);

        //TODO: get metadata from each of the extractors
        VolumeTradedWithEntityYTDExtractor extractor = new VolumeTradedWithEntityYTDExtractor();

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, sessions, trades_df);

        Object result = meta.get(RfqMetadataFieldNames.volumeTradedYearToDate);
        System.out.println(result);
        //TODO: publish the metadata

    }
}
