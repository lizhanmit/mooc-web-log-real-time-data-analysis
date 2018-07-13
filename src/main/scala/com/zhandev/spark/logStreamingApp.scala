package com.zhandev.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import _root_.kafka.serializer.StringDecoder

/**
  * Integrate Spark Streaming and Kafka to do word count in direct approach.
  */
object logStreamingApp {

  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      System.err.println("Usage: logStreamingApp <brokers> <topics>")
      System.exit(1)
    }

    val Array(brokers, topics) = args

    // local mode
    val sparkConf = new SparkConf().setAppName("logStreamingApp").setMaster("local[2]")

    // server mode (in real projects)
    //val sparkConf = new SparkConf()

    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)

    // TODO... Spark Streaming integrates Kafka
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet
    )

    messages.map(_._2).count().print()

    ssc.start()
    ssc.awaitTermination()
  }
}
