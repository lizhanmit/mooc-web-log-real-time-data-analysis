package com.zhandev.spark.kafka;

/**
 * Kafka common properties
 */
public class KafkaProperties {

    public static final String ZK = "10.0.2.15:2181"; // zookeeper connect

    public static final String TOPIC = "hello_topic";

    public static final String BROKER_LIST = "10.0.2.15:9092"; // kafka listens on

    public static final String GROUP_ID = "test_group1";

}
