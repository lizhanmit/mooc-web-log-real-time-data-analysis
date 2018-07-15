package com.zhandev.spark.project.spark

import _root_.kafka.serializer.StringDecoder
import com.zhandev.spark.project.domain.ClickLog
import com.zhandev.spark.project.utils.DateUtils
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Spark Streaming real project
  * Spark Streaming receives web log and then does processing
  */
object StatStreamingProjectApp {

  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      System.err.println("Usage: StatStreamingProjectApp <brokers> <topics>")
      System.exit(1)
    }

    val Array(brokers, topics) = args

    // local mode
    val sparkConf = new SparkConf().setAppName("logStreamingApp").setMaster("local[2]")

    // server mode (in real projects)
    //val sparkConf = new SparkConf()

    val ssc = new StreamingContext(sparkConf, Seconds(60))

    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)

    // TODO... Spark Streaming integrates Kafka
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet
    )

    // test that messages can be received successfully
    //messages.map(_._2).count().print()

    // data cleansing
    val logs = messages.map(_._2)
    val cleanData = logs.map(line => {
      val info = line.split("\t") // info = (63.55.187.143	2018-07-15 17:30:01	"GET /class/146.html HTTP/1.1"	404	https://search.yahoo.com/search?p=Big Data Interview Skills)

      // info(2) = "GET /class/146.html HTTP/1.1"
      // url = /class/146.html
      val url = info(2).split(" ")(1)
      var courseId = 0

      // get courseId of class type course
      if (url.startsWith("/class")) {
        val courseIdHTML = url.split("/")(2) // courseIdHTML = 146.html
        courseId = courseIdHTML.substring(0, courseIdHTML.lastIndexOf(".")).toInt // courseId = 146
      }

      ClickLog(info(0), DateUtils.parseToMinute(info(1)), courseId, info(3).toInt, info(4))
    }).filter(clicklog => clicklog.courseId != 0).print()

    ssc.start()
    ssc.awaitTermination()
  }
}
