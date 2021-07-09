package com.cs.rfq.utils;


import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.google.common.io.Resources;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;

public class TopicCreator
{
    public static void main(String[] args) throws Exception {
        String path = Resources.getResource("kafka.properties").getPath();
        Properties properties = new Properties();
        properties.load(new FileReader(new File(path)));

        AdminClient adminClient = AdminClient.create(properties);
        NewTopic newTopic = new NewTopic("topicName", 1, (short)1); //new NewTopic(topicName, numPartitions, replicationFactor)

        List<NewTopic> newTopics = new ArrayList<NewTopic>();
        newTopics.add(newTopic);

        adminClient.createTopics(newTopics);
        adminClient.close();
    }
}
