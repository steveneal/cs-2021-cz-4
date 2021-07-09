package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;

import java.util.HashMap;
import java.util.Map;

public class TotalVolumeTradedForInstrumentExtractor implements RfqMetadataExtractor {

    private String since;

    public TotalVolumeTradedForInstrumentExtractor() {
        // Set Last Traded Date for past four years due to lack for data
        this.since = (DateTime.now().getYear()-4) + "-01-01";
    }

    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {

        trades.createOrReplaceTempView("trade");
        String query = String.format("SELECT sum(LastQty) from trade where TraderId='%s' AND TradeDate >= '%s'",
                rfq.getTraderId(), since);

        Dataset<Row> sqlQueryResults = session.sql(query);

        Object volume = sqlQueryResults.first().get(0);
        if (volume == null) {
            volume = 0L;
        }
        System.out.println("VOLUME "+volume+" - Trades length "+trades.count() + "Trader ID - " + rfq.getTraderId());
        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();
        results.put(RfqMetadataFieldNames.totalVolumeTradedForInstrument, volume);

        return results;
    }

    protected void setSince(String since) {
        this.since = since;
    }
}
