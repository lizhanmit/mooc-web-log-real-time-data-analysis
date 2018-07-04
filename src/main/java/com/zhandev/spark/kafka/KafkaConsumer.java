package com.zhandev.spark.kafka;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Kafka consumer
 */
public class KafkaConsumer extends Thread { // use Thread to test (consume messages) so extends Thread here

    private String topic;

    public KafkaConsumer(String topic) {
        this.topic = topic;
    }

    private ConsumerConnector createConnect() {
        Properties properties = new Properties();
        properties.put("zookeeper.connect", KafkaProperties.ZK);
        properties.put("group.id", KafkaProperties.GROUP_ID);
        return Consumer.createJavaConsumerConnector(new ConsumerConfig(properties));
    }

    @Override
    public void run() {
        ConsumerConnector consumer = createConnect();

        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, 1); // 1: number of KafkaStream
        // you may have other topics
//        topicCountMap.put(topic2, 1);
//        topicCountMap.put(topic3, 1);

        // String: topic
        // List<KafkaStream<byte[], byte[]>>: data stream corresponding to the topic
        Map<String, List<KafkaStream<byte[], byte[]>>> messageStream = consumer.createMessageStreams(topicCountMap);

        KafkaStream<byte[], byte[]> stream = messageStream.get(topic).get(0); // get data received with the topic you specify (here it is topic)

        ConsumerIterator<byte[], byte[]> iterator = stream.iterator();

        while (iterator.hasNext()) {
            String message = new String(iterator.next().message());
            System.out.println("receive: " + message);
        }

    }
}
