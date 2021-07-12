package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import com.cs.rfq.decorator.TradeDataLoader;
import dev.testdata.Instrument;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import static org.junit.Assert.assertEquals;

public class TradeBiasTest extends AbstractSparkUnitTest{
    private Rfq rfq1;
    private Rfq rfq2;

    @Before
    public void setup() {
        rfq1 = new Rfq();
        rfq1.setEntityId(5561279226039690843L);
        rfq1.setIsin("AT0000A0VRQ6");
        rfq2 = new Rfq();
        rfq2.setEntityId(5561279226039690843L);
        rfq2.setIsin("AT0000A0VRQ7");
    }

    @Test
    public void checkInstrumentLiquidityWhenAllTradesMatch() {

        //String filePath = getClass().getResource("volume-traded-1.json").getPath();
        Dataset<Row> trades = new TradeDataLoader().loadTrades(session, "src/test/resources/trades/trades.json");

        TradeSideBiasExtractor extractor = new TradeSideBiasExtractor();

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq1, session, trades);

        Object result = meta.get(RfqMetadataFieldNames.tradeBiasMonthToDate);

        assertEquals(1L, result);
    }

    @Test
    public void checkInstrumentLiquidityWhenNoTradesMatch() {

        String filePath = getClass().getResource("volume-traded-1.json").getPath();
        Dataset<Row> trades = new TradeDataLoader().loadTrades(session, filePath);

        //all test trade data are for 2018 so this will cause no matches
        TradeSideBiasExtractor extractor = new TradeSideBiasExtractor();

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq2, session, trades);

        Object result = meta.get(RfqMetadataFieldNames.tradeBiasMonthToDate);

        assertEquals(new Long(-1), result);
    }
}
