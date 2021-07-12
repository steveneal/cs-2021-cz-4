package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;

import java.util.HashMap;
import java.util.Map;

public class AverageTradedPriceExtractor implements RfqMetadataExtractor {

    private java.sql.Date since;

    public AverageTradedPriceExtractor() {
        this.since = new java.sql.Date(new DateTime().minusMonths(1).getMillis());
    }

    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {

        trades.createOrReplaceTempView("trade");
        String query = String.format("SELECT AVG(LastPx) from trade where SecurityID='%s' AND TradeDate >= '%s'",
                rfq.getIsin(), since);

        Dataset<Row> sqlQueryResults = session.sql(query);

        Object volume = sqlQueryResults.first().get(0);
        if (volume == null) {
            volume = 0L;
        }
        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();
        results.put(RfqMetadataFieldNames.averageTradedPrice, volume);

        return results;
    }

    protected void setSince(java.sql.Date since) {
        this.since = since;
    }
}
