package com.cs.rfq.decorator;

import com.sun.org.apache.xpath.internal.operations.String;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.spark.sql.types.DataTypes.*;

public class TradeDataLoader {

    private final static Logger log = LoggerFactory.getLogger(TradeDataLoader.class);

    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "C:\\Java\\hadoop-2.9.2");
        System.setProperty("spark.master", "local");

        SparkSession session = SparkSession.builder()
                .appName("Dataset with SQL")
                .getOrCreate();
        //TODO: create an explicit schema for the trade data in the JSON files
        StructType schema =
                new StructType(new StructField[] {
                        new StructField("TraderId", IntegerType, false, Metadata.empty()),
                        new StructField("EntityId", IntegerType, false, Metadata.empty()),
                        new StructField("MsgType", IntegerType, false, Metadata.empty()),
                        new StructField("TradeReportId", IntegerType, false, Metadata.empty()),
                        new StructField("PreviouslyReported", StringType, false, Metadata.empty()),
                        new StructField("SecurityID", StringType, false, Metadata.empty()),
                        new StructField("SecurityIdSource", IntegerType, false, Metadata.empty()),
                        new StructField("LastQty", IntegerType, false, Metadata.empty()),
                        new StructField("LastPx", IntegerType, false, Metadata.empty()),
                        new StructField("TradeDate", StringType, false, Metadata.empty()),
                        new StructField("TransactTime", StringType, false, Metadata.empty()),
                        new StructField("NoSides", IntegerType, false, Metadata.empty()),
                        new StructField("Side", IntegerType, false, Metadata.empty()),
                        new StructField("OrderID", IntegerType, false, Metadata.empty()),
                        new StructField("Currency", StringType, false, Metadata.empty())
                });

        //TODO: load the trades datasetsss
        Dataset<Row> trades = session.read().schema(schema).json("src/test/resources/trades/trades.json");

        //TODO: log a message indicating number of records loaded and the schema used

        trades.printSchema();
        trades.createOrReplaceTempView("trades");
        session.sql("SELECT COUNT(*) FROM trades").show();
    }

    public static Dataset<Row> loadTrades(SparkSession session, String path) {
        //TODO: create an explicit schema for the trade data in the JSON files
        StructType schema =
                new StructType(new StructField[] {
                        new StructField("TraderId", IntegerType, false, Metadata.empty()),
                        new StructField("EntityId", IntegerType, false, Metadata.empty()),
                        new StructField("MsgType", IntegerType, false, Metadata.empty()),
                        new StructField("TradeReportId", IntegerType, false, Metadata.empty()),
                        new StructField("PreviouslyReported", StringType, false, Metadata.empty()),
                        new StructField("SecurityID", StringType, false, Metadata.empty()),
                        new StructField("SecurityIdSource", IntegerType, false, Metadata.empty()),
                        new StructField("LastQty", IntegerType, false, Metadata.empty()),
                        new StructField("LastPx", IntegerType, false, Metadata.empty()),
                        new StructField("TradeDate", StringType, false, Metadata.empty()),
                        new StructField("TransactTime", StringType, false, Metadata.empty()),
                        new StructField("NoSides", IntegerType, false, Metadata.empty()),
                        new StructField("Side", IntegerType, false, Metadata.empty()),
                        new StructField("OrderID", IntegerType, false, Metadata.empty()),
                        new StructField("Currency", StringType, false, Metadata.empty())
                });

        //TODO: load the trades datasetsss
        Dataset<Row> trades = session.read().schema(schema).json("src/test/resources/trades/trades.json");

        //TODO: log a message indicating number of records loaded and the schema used

        trades.printSchema();
        trades.createOrReplaceTempView("trades");
        session.sql("SELECT COUNT(*) FROM trades").show();

        return null;
    }

}
