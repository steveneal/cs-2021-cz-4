package com.cs.rfq.decorator;

import com.cs.rfq.decorator.extractors.*;
import com.cs.rfq.decorator.publishers.MetadataJsonLogPublisher;
import com.cs.rfq.decorator.publishers.MetadataPublisher;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.json.JSONObject;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import java.util.*;

import static org.apache.spark.sql.functions.sum;

public class RfqProcessor {

    private final static Logger log = LoggerFactory.getLogger(RfqProcessor.class);

//    public final SparkSession sessions
//
//    private final JavaStreamingContext streamingContext;

//    public final Dataset<Row> trades_df

    private final List<RfqMetadataExtractor> extractors = new ArrayList<>();

    private final MetadataPublisher publisher = new MetadataJsonLogPublisher();

    private  Map instrumentMap;


    public RfqProcessor(SparkSession sessions, JavaStreamingContext streamingContext) throws Exception{
//        this.session = session;
//        this.streamingContext = streamingContext;
        String server = "127.0.0.1:9092";
        String groupId = "some_application";
        String topic = "topicName";
        //TODO: use the TradeDataLoader to load the trade data archives
        TradeDataLoader trades = new TradeDataLoader();
        Dataset <Row> trades_df = trades.loadTrades(sessions, "src/test/resources/trades/trades.json");
        //TODO: take a close look at how these two extractors are implemented
        extractors.add(new TotalTradesWithEntityExtractor());
        extractors.add(new VolumeTradedWithEntityYTDExtractor());
        extractors.add(new TotalVolumeTradedForInstrumentExtractor());

        Consumer rfqKafkaReceiver = new Consumer(server, groupId, topic);
        rfqKafkaReceiver.execute(sessions,trades_df);
        //startSocketListener(sessions, streamingContext, trades_df);
    }

    public static void startSocketListener(SparkSession sessions, JavaStreamingContext jssc, Dataset<Row> trades_df) throws InterruptedException {
        //TODO: stream data from the input socket on localhost:9000

        /*JavaDStream<String> lines = jssc.socketTextStream("localhost", 9000);
        //JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(x.split(" ")).iterator());

        //TODO: convert each incoming line to a Rfq object and call processRfq method with it
        lines.foreachRDD(rdd -> {
            rdd.collect().forEach(line -> processRfq(Rfq.fromJson(line.toString()), sessions, trades_df));
        });


        //TODO: start the streaming context
        jssc.start();
        jssc.awaitTermination();*/
        String server = "127.0.0.1:9092";
        String groupId = "some_application";
        String topic = "topicName";


    }

    public static void processRfq(Rfq rfq, SparkSession sessions, Dataset<Row> trades_df) {
        log.info(String.format("Received Rfq: %s", rfq.toString()));

        //create a blank map for the metadata to be collected
        Map<RfqMetadataFieldNames, Object> metadata = new HashMap<>();
        //String liquidty = metadata.lookupLiquidity(rfq.getIsin());
        //metadata.put(RfqMetadataFieldNames.instrumentLiquidity, liquidty);

        //TODO: get metadata from each of the extractors
        // Extractors
        VolumeTradedWithEntityYTDExtractor extractor = new VolumeTradedWithEntityYTDExtractor();
        TotalVolumeTradedForInstrumentExtractor volumeTradedForInstrumentExtractor = new TotalVolumeTradedForInstrumentExtractor();
        InstrumentLiquidityExtractor instrumentLiquidityExtractor = new InstrumentLiquidityExtractor();
        //Map object
        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, sessions, trades_df);
        Map<RfqMetadataFieldNames, Object> volumeTradedForInstrumentExtractorMeta = volumeTradedForInstrumentExtractor.extractMetaData(rfq, sessions, trades_df);
        Map<RfqMetadataFieldNames, Object> instrumentLiquiditMeta = instrumentLiquidityExtractor.extractMetaData(rfq, sessions, trades_df);
        // Calculating Meta data
        Long volume = (Long) volumeTradedForInstrumentExtractorMeta.get(RfqMetadataFieldNames.totalVolumeTradedForInstrument);
        Long instrumentLiqudity = (Long) instrumentLiquiditMeta.get(RfqMetadataFieldNames.instrumentLiquidity);
        // Assign meta data
        metadata.put(RfqMetadataFieldNames.totalVolumeTradedForInstrument, volume);
        metadata.put(RfqMetadataFieldNames.instrumentLiquidity, instrumentLiqudity);

        //TODO: publish the metadata
    }


    public static class Consumer {
        private final Logger mLogger = LoggerFactory.getLogger(Consumer.class.getName());
        private final String mBootstrapServer;
        private final String mGroupId;
        private final String mTopic;

        public Consumer(String bootstrapServer, String groupId, String topic) {
            mBootstrapServer = bootstrapServer;
            mGroupId = groupId;
            mTopic = topic;
        }

        /*public static void main(String[] args) {
            String server = "127.0.0.1:9092";
            String groupId = "some_application";
            String topic = "topicName";

            new com.cs.rfq.utils.Consumer(server, groupId, topic).execute();
        }*/
        public void execute(SparkSession sessions, Dataset<Row> trades_df){
            mLogger.info("Creating consumer thread");

            CountDownLatch latch = new CountDownLatch(1);

            Consumer.ConsumerRunnable consumerRunnable = new Consumer.ConsumerRunnable(mBootstrapServer, mGroupId, mTopic, latch, sessions,  trades_df);
            Thread thread = new Thread(consumerRunnable);
            thread.start();

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                mLogger.info("Caught shutdown hook");
                consumerRunnable.shutdown();
                await(latch);

                mLogger.info("Application has exited");
            }));

            await(latch);

        }
        private void await(CountDownLatch latch) {
            try {
                latch.await();
            } catch (InterruptedException e) {
                mLogger.error("Application got interrupted", e);
            } finally {
                mLogger.info("Application is closing");
            }
        }
        //Inner class to run consumer on separated thead
        private class ConsumerRunnable implements Runnable{
            private CountDownLatch mLatch;
            private KafkaConsumer<String, String> mConsumer;
            private SparkSession sessions;
            private Dataset<Row> trades_df;

            ConsumerRunnable(String bootstrapServer, String groupId, String topic, CountDownLatch latch,SparkSession sessions, Dataset<Row> trades_df) {
                mLatch = latch;
                this.sessions=sessions;
                this.trades_df=trades_df;
                Properties props = consumerProps(bootstrapServer, groupId);
                mConsumer = new KafkaConsumer<>(props);
                mConsumer.subscribe(Collections.singletonList(topic));
            }

            private Properties consumerProps(String bootstrapServer, String groupId) {
                String deserializer = StringDeserializer.class.getName();
                Properties properties = new Properties();
                properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
                properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
                properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, deserializer);
                properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserializer);
                properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

                return properties;
            }
            private void storeRfqToJson(String rfq) throws IOException {
                mLogger.info("Storing to rfqStore");
                FileWriter fileWriter = new FileWriter("src/main/resources/streaming/rfqStore.txt", true);
                PrintWriter printWriter = new PrintWriter(fileWriter);
                printWriter.println(rfq);
                printWriter.close();
                mLogger.info(rfq+" was stored");
            }

            @Override
            public void run() {
                //we fetch data from topic every 100 milliseconds
                try {
                    while (true) {
                        ConsumerRecords<String, String> records = mConsumer.poll(Duration.ofMillis(100));

                        for (ConsumerRecord<String, String> record : records) {
                            //RFQ Processing happens here
                            mLogger.info("Processing RFQs");
                            mLogger.info("Key: " + record.key() + ", Value: " + record.value());
                            mLogger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                            storeRfqToJson(record.value());
                            JSONObject jsonObject = new JSONObject(record.value());
                            String values = jsonObject.getJSONObject("value").toString();
                            //String value = jsonObject.getString("value");
                            processRfq(Rfq.fromJson(values),sessions,trades_df);
                        }
                    }
                } catch (WakeupException e) {
                    mLogger.info("Received shutdown signal!");
                } catch (IOException e) {
                    e.printStackTrace();
                } finally {
                    mConsumer.close();
                    mLatch.countDown();
                }
            }

            public void shutdown(){
                mConsumer.wakeup();
            }

        }
    }

}

