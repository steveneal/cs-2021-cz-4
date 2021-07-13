package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;

import java.util.HashMap;
import java.util.Map;

public class TradeSideBiasExtractor implements RfqMetadataExtractor {

    private java.sql.Date sincemonth;
    private java.sql.Date sinceweek;
    private Object TradeBiasMTD;
    private Object TradeBiasWTD;

    public TradeSideBiasExtractor() {
        long month = new DateTime().minusMonths(1).getMillis();
        this.sincemonth = new java.sql.Date(month);
        long week = new DateTime().minusWeeks(1).getMillis();
        this.sinceweek = new java.sql.Date(week);
    }

    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {
        String buySideMonth = String.format("SELECT SUM(LastQty) from trade where EntityId='%s' AND SecurityId='%s' AND TradeDate >= '%s' AND Side = 1",
            rfq.getEntityId(),
            rfq.getIsin(),
            sincemonth);
        String sellSideMonth = String.format("SELECT SUM(LastQty) from trade where EntityId='%s' AND SecurityId='%s' AND TradeDate >= '%s' AND Side = 2",
                rfq.getEntityId(),
                rfq.getIsin(),
                sincemonth);
        trades.createOrReplaceTempView("trade");
        Object BuySideMonth = session.sql(buySideMonth).first().get(0);
        Object SellSideMonth = session.sql(sellSideMonth).first().get(0);
        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();

        if (BuySideMonth == null || SellSideMonth == null){
            TradeBiasMTD = -1L;
            results.put(RfqMetadataFieldNames.tradeBiasMonthToDate, TradeBiasMTD);
        }else{
            TradeBiasMTD = (Long) BuySideMonth / (Long)SellSideMonth;
            results.put(RfqMetadataFieldNames.tradeBiasMonthToDate, TradeBiasMTD);
        }
        String buySideWeek = String.format("SELECT SUM(LastQty) from trade where EntityId='%s' AND SecurityId='%s' AND TradeDate >= '%s' AND Side = 1",
                rfq.getEntityId(),
                rfq.getIsin(),
                sinceweek);
        String sellSideWeek = String.format("SELECT SUM(LastQty) from trade where EntityId='%s' AND SecurityId='%s' AND TradeDate >= '%s' AND Side = 2",
                rfq.getEntityId(),
                rfq.getIsin(),
                sinceweek);
        Object BuySideWeek = session.sql(buySideWeek).first().get(0);
        Object SellSideWeek = session.sql(sellSideWeek).first().get(0);
        if (BuySideWeek == null || SellSideWeek == null){
            TradeBiasWTD = -1L;
            results.put(RfqMetadataFieldNames.tradeBiasWeekToDate, TradeBiasWTD);
        }else{
            TradeBiasWTD = (Long) BuySideWeek / (Long)SellSideWeek;
            results.put(RfqMetadataFieldNames.tradeBiasWeekToDate, TradeBiasWTD);
        }
        return results;
    }
    protected void setSinceweek(java.sql.Date sinceweek) {
        this.sinceweek = sinceweek;
    }
    protected void setSincemonth(java.sql.Date sincemonth) {
        this.sincemonth = sincemonth;
    }
}
