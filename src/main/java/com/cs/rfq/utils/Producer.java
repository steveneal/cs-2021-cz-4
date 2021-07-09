package com.cs.rfq.utils;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shapeless.ops.nat;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class Producer {

    final KafkaProducer<String, String> mProducer;
    final Logger mLogger = LoggerFactory.getLogger(Producer.class);

    Producer(String bootstrapServer){
        Properties props = producerProps(bootstrapServer);
        mProducer = new KafkaProducer<>(props);
        mLogger.info("Producer On");
    }
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        String server = "127.0.0.1:9092";
        String topic = "topicName";
        String rfq1 = rfqGenerator("rfq3","7514623710987345033","AT0000383864",3351266293154445953L,5561279226039690843L,250000L,1.58,"B");
        String rfq2 = rfqGenerator("rfq4","7514623710987345250","AT0000383250",33512662931544L,5561279226039L,650000L,2.63,"A");
        Producer producer = new Producer(server);
        producer.put(topic, "rfq3", rfq1);
        producer.put(topic, "rfq4", rfq2);
        producer.close();


    }

    private static String rfqGenerator(String key, String id, String isin, Long traderId, Long entityId, Long quantity, Double price, String side){
        return "{key:"+key+", value:{" +
                "id:'" + id + '\'' +
                ", isin:'" + isin + '\'' +
                ", traderId:" + traderId +
                ", entityId:" + entityId +
                ", quantity:" + quantity +
                ", price:" + price +
                ", side:" + side +
                "}}";
    }

    public void put(String topic, String key, String value) throws ExecutionException, InterruptedException {
        mLogger.info("Put value: " + value + ", for key: " + key);

        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
        mProducer.send(record, (recordMetadata, e) -> {
            if (e != null) {
                mLogger.error("Error while producing", e);
                return;
            }

            mLogger.info("Received new meta. Topic: " + recordMetadata.topic()
                    + "; Partition: " + recordMetadata.partition()
                    + "; Offset: " + recordMetadata.offset()
                    + "; Timestamp: " + recordMetadata.timestamp());
        }).get();
    }

    public void close() {
        mLogger.info("Closing producer's connection");
        mProducer.close();
    }

    private Properties producerProps(String bootstrapServer){
        String serializer = StringSerializer.class.getName();
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, serializer);
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, serializer);
        return props;
    }
}
