package com.cs.rfq.decorator;

import com.cs.rfq.decorator.extractors.RfqMetadataFieldNames;
import com.cs.rfq.decorator.extractors.TotalVolumeTradedForInstrumentExtractor;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.*;

public class RfqProcessorTest {
    static SparkSession session;
    static SparkConf conf;
    static Dataset<Row> trades_df;

    @BeforeClass
    public static void setup() throws Exception {
        System.setProperty("hadoop.home.dir", "C:\\Java\\hadoop-2.9.2");
        System.setProperty("spark.master", "local[4]");

        //TODO: create a Spark configuration and set a sensible app name
        conf = new SparkConf().setAppName("RFQDecorator");
        //TODO: create a Spark streaming context
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.milliseconds(100));
        //TODO: create a Spark session
        session = SparkSession.builder().config(conf).getOrCreate();
        //TODO: create a new RfqProcessor and set it listening for incoming RFQs
        TradeDataLoader trades = new TradeDataLoader();
        trades_df = trades.loadTrades(session, "src/test/resources/trades/trades.json");
    }

    @Test
    public void checkLiquidityMetadata(){
        RfqMetadata rfqMetadata = new RfqMetadata();
        String liquidity = rfqMetadata.lookupLiquidity("AT0000A0U3T4");
        String liquidityKeyErorr = rfqMetadata.lookupLiquidity("A most likely non-existing instrument");
        assertEquals("114.44", liquidity);
        assertEquals("-", liquidityKeyErorr);
    }

    @Test
    public void checkMetadataTotalVolumeTradedForInstrument(){
        Rfq testMessage = new Rfq();
        testMessage.setTraderId(3351266293154445953L);
        TotalVolumeTradedForInstrumentExtractor volumeTradedForInstrumentExtractor = new TotalVolumeTradedForInstrumentExtractor();
        Map<RfqMetadataFieldNames, Object> volumeTradedForInstrumentExtractorMeta = volumeTradedForInstrumentExtractor.extractMetaData(testMessage, session, trades_df);
        Long volume = (Long) volumeTradedForInstrumentExtractorMeta.get(RfqMetadataFieldNames.totalVolumeTradedForInstrument);
        assertEquals((Long) volume,(Long) 20300000L);
    }
}
