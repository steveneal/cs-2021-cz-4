package com.cs.rfq.decorator;

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

    private static Map processInstrumentData(){
        // create a reader
        Map<String, String> instrumentMap = new HashMap();
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
                instrumentMap.put(instrumentId, priceLast);
            }

        } catch (IOException ex) {
            ex.printStackTrace();
        }
        System.out.println("DICT LENGHT: "+instrumentMap.size());
        return instrumentMap;
    }
}
