package com.cs.rfq.decorator;

import com.cs.rfq.decorator.extractors.InstrumentLiquidityExtractor;
import com.cs.rfq.decorator.extractors.RfqMetadataFieldNames;
import com.cs.rfq.decorator.extractors.TotalVolumeTradedForInstrumentExtractor;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
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

    @Test
    public void checkInstrumentLiquidityTest(){
        Rfq testMessage = new Rfq();
        testMessage.setTraderId(1509345351319978288L);
        testMessage.setIsin("AT0000A0N9A0");
        // {'TraderId':1509345351319978288, 'EntityId':5561279226039690843, 'MsgType':35, 'TradeReportId':6508027238640898712, 'PreviouslyReported':'N', 'SecurityID':'AT0000A0N9A0', 'SecurityIdSource':4, 'LastQty':400000, 'LastPx':115.247, 'TradeDate':'2021-07-10', 'TransactTime':'20210710-16:41:30', 'NoSides':1, 'Side':2, 'OrderID':3635806187320116526, 'Currency':'EUR'}
        InstrumentLiquidityExtractor instrumentLiquidityExtractor = new InstrumentLiquidityExtractor();
        Map<RfqMetadataFieldNames, Object> instrumentLiquidityExtractorMeta = instrumentLiquidityExtractor.extractMetaData(testMessage, session, trades_df);
        Long instrumentLiquidity = (Long) instrumentLiquidityExtractorMeta.get(RfqMetadataFieldNames.instrumentLiquidity);
        assertEquals((Long) instrumentLiquidity,(Long) 400000L);
    }
}
