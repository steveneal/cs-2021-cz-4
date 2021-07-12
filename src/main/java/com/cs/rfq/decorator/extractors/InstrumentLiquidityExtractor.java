package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;

import java.util.HashMap;
import java.util.Map;

public class InstrumentLiquidityExtractor implements RfqMetadataExtractor {

    private String since;

    public InstrumentLiquidityExtractor() {
        String month = String.format("%02d", (DateTime.now().getMonthOfYear()));
        this.since = (DateTime.now().getYear()) + "-"+ month + "-01";
    }

    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {

        trades.createOrReplaceTempView("trade");
        String query = String.format("SELECT sum(LastQty) from trade where TraderId='%s' And SecurityID='%s' AND TradeDate >= '%s'",
                rfq.getTraderId(), rfq.getIsin() ,since);

        Dataset<Row> sqlQueryResults = session.sql(query);

        Object volume = sqlQueryResults.first().get(0);
        if (volume == null) {
            volume = 0L;
        }
        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();
        results.put(RfqMetadataFieldNames.instrumentLiquidity, volume);

        return results;
    }

    protected void setSince(String since) {
        this.since = since;
    }
}
