package com.cs.rfq.decorator.publishers;

import com.cs.rfq.decorator.extractors.RfqMetadataFieldNames;
import com.cs.rfq.utils.Producer;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ExecutionException;

public class MetadataJsonLogPublisher implements MetadataPublisher {

    private static final Logger log = LoggerFactory.getLogger(MetadataJsonLogPublisher.class);

    @Override
    public void publishMetadata(Map<RfqMetadataFieldNames, Object> metadata) {
        String s = new GsonBuilder().setPrettyPrinting().create().toJson(metadata);
        log.info(String.format("Publishing metadata:%n%s", s));
        String publishServer = "127.0.0.1:9092";
        String publishTopic = "downstreamTest";
        /*
        Producer metadataPublisher = new Producer(publishServer);
        try {
            metadataPublisher.put(publishTopic, "testKey", s);
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

         */
    }
}
