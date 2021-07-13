package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;

import java.util.HashMap;
import java.util.Map;

public class TotalVolumeTradedByEntityExtractor implements RfqMetadataExtractor {

    private long since;
    private long sincemonth;
    private long sinceweek;

    public TotalVolumeTradedByEntityExtractor() {
        this.sincemonth = new DateTime().minusMonths(1).getMillis();
        this.sinceweek = new DateTime().minusWeeks(1).getMillis();
        this.since = DateTime.now().minusYears(1).getMillis();
    }

    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {

        trades.createOrReplaceTempView("trade");
        String queryYTD = String.format("SELECT sum(LastQty) from trade where EntityId='%s' AND SecurityID='%s' AND TradeDate >= '%s'",
                rfq.getEntityId(), rfq.getIsin(), new java.sql.Date(since));
        String queryMTD = String.format("SELECT sum(LastQty) from trade where EntityId='%s' AND SecurityID='%s' AND TradeDate >= '%s'",
                rfq.getEntityId(), rfq.getIsin(), new java.sql.Date(sincemonth));
        String queryWTD = String.format("SELECT sum(LastQty) from trade where EntityId='%s' AND SecurityID='%s' AND TradeDate >= '%s'",
                rfq.getEntityId(), rfq.getIsin(), new java.sql.Date(sinceweek));
        Object volumeYTD = session.sql(queryYTD).first().get(0);
        Object volumeMTD = session.sql(queryMTD).first().get(0);
        Object volumeWTD = session.sql(queryWTD).first().get(0);

        if (volumeYTD == null) {
            volumeYTD = 0L;
        }
        if (volumeMTD == null) {
            volumeMTD = 0L;
        }
        if (volumeWTD == null) {
            volumeWTD = 0L;
        }
        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();
        results.put(RfqMetadataFieldNames.tradesWithEntityPastYear, volumeYTD);
        results.put(RfqMetadataFieldNames.tradesWithEntityPastMonth, volumeMTD);
        results.put(RfqMetadataFieldNames.tradesWithEntityPastWeek, volumeWTD);
        return results;
    }

    protected void setSince(long since) {
        this.since = since;
        this.sincemonth = sincemonth;
        this.sinceweek = sinceweek;
    }
}
