package com.zhandev.spark.kafka;

/**
 * Kafka Java API test
 */
public class KafkaClientApp {

    public static void main(String[] args) {

        new KafkaProducer(KafkaProperties.TOPIC).start();

        new KafkaConsumer(KafkaProperties.TOPIC).start();
    }
}
