package com.cs.rfq.decorator;

import org.apache.spark.sql.SparkSession;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

public class RfqMetadata {

    private static  Map instrumentMap;

    public  RfqMetadata(){
        this.instrumentMap = processInstrumentData();
    }

    public String lookupLiquidity(String instrumentId){
        String lastPrice;
        lastPrice = (String) this.instrumentMap.get(instrumentId);

        if (lastPrice == null){
            lastPrice = "-";
        }
        return lastPrice;
    }

    public static void getVolume(String traderId){
        /*
        TradeDataLoader tradeDataLoader = new TradeDataLoader();
        SparkSession sparkSession = tradeDataLoader.getSession();


        tring query = String.format("SELECT sum(LastQty) from trade where EntityId='%s' AND SecurityId='%s' AND TradeDate >= '%s'",
                rfq.getEntityId(),

        String sqlQuery = String.format("select sum(lastQty) from trades where TraderId = '%s'", traderId);
        sparkSession.sql(sqlQuery).show();
        */
    }

    private static Map processInstrumentData(){
        // create a reader
        Map<String, String> map = new HashMap();
        try (BufferedReader br = Files.newBufferedReader(Paths.get("src/test/resources/trades/instrument-static.csv"))) {

            // CSV file delimiter
            String DELIMITER = ",";
            // read the file line by line
            String line;
            while ((line = br.readLine()) != null) {
                // convert line into columns
                String[] columns = line.split(DELIMITER);
                // print all columns
                String instrumentId = columns[0];
                String priceLast = columns[9];
                map.put(instrumentId, priceLast);
            }

        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return map;
    }
}
