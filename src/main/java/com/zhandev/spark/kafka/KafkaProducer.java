package com.zhandev.spark.kafka;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

/**
 * Kafka producer
 */
public class KafkaProducer extends Thread { // use Thread to test (send messages) so extends Thread here

    private String topic;
    private Producer<Integer, String> producer;

    public KafkaProducer(String topic) {
        this.topic = topic;

        Properties properties = new Properties();
        properties.put("metadata.broker.list", KafkaProperties.BROKER_LIST);
        properties.put("serializer.class", "kafka.serializer.StringEncoder");
        // How many replicas must acknowledge a message before its considered successfully written.
        // Accepted values are:
        // 0 (Never wait for acknowledgement),
        // 1 (wait for leader only),
        // -1 (wait for all replicas)
        // Set this to -1 to avoid data loss in some cases of leader failure.
        properties.put("request.required.acks", "1"); //

        producer = new Producer<Integer, String>(new ProducerConfig(properties));
    }

    @Override
    public void run() {
        int messageNo = 1;

        while (true) {
            String message = "message_" + messageNo;
            producer.send(new KeyedMessage<Integer, String>(topic, message));
            System.out.println("Sent: " + message);

            messageNo ++;

            try {
                Thread.sleep(2000);
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }
}
