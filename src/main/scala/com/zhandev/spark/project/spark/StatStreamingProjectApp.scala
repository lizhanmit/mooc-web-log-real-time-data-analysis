package com.zhandev.spark.project.spark

import _root_.kafka.serializer.StringDecoder
import com.zhandev.spark.project.dao.{ClassTypeCourseClickCountDao, ClassTypeCourseSearchClickCountDao}
import com.zhandev.spark.project.domain.{ClassTypeCourseClickCount, ClassTypeCourseSearchClickCount, ClickLog}
import com.zhandev.spark.project.utils.DateUtils
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

/**
  * Spark Streaming real project.
  * Spark Streaming receives web log from Kafka and then does data processing.
  */
object StatStreamingProjectApp {

  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      System.err.println("Usage: StatStreamingProjectApp <brokers> <topics>")
      System.exit(1)
    }

    val Array(brokers, topics) = args

    // local mode
    //val sparkConf = new SparkConf().setAppName("logStreamingApp").setMaster("local[2]")

    // server mode (in real projects)
    val sparkConf = new SparkConf()

    val ssc = new StreamingContext(sparkConf, Seconds(60))

    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)

    /**
      * Spark Streaming integrates Kafka to get data.
      */
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet
    )

    // test that messages can be received successfully
    //messages.map(_._2).count().print()

    /**
      * Data cleansing.
      */
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
    }).filter(clicklog => clicklog.courseId != 0)

    // for testing
    //cleanData.print()

    /**
      * Save data into HBase.
      * The data is about page view (click count) of class type courses today up to now.
      *
      * 1. Transfer clean ClickLog data to (day_courseId, 1) pairs.
      * 2. For each pair, do reduce, then get DStreams. (word count)
      * 3. For each DStream, get iterators.
      * 4. For each iterator, get pair.
      * 5. Transfer pair to ClassTypeCourseClickCount obj, then add to the list of ClassTypeCourseClickCount.
      * 6. Save the list into HBase using ClassTypeCourseClickCountDao save() function.
      */
    cleanData.map(x => {(x.time.substring(0, 8) + "_" + x.courseId, 1)})  // Transfer clean ClickLog data to (day_courseId, 1) pairs.
              .reduceByKey(_+_)  // For each pair, do reduce, then get DStream.
              .foreachRDD(rdd => {rdd.foreachPartition(partitionRecords => {  // For each DStream, get iterators.
                  val list = new ListBuffer[ClassTypeCourseClickCount]
                  partitionRecords.foreach(pair => {  // For each iterator, get pair.
                    list.append(ClassTypeCourseClickCount(pair._1, pair._2))  // Transfer pair to ClassTypeCourseClickCount obj, then add to the list of ClassTypeCourseClickCount.
                  })

                ClassTypeCourseClickCountDao.save(list)  // Save the list into HBase using ClassTypeCourseClickCountDao.save() function.
                })
              })


    /**
      * Save data into HBase.
      * The data is about page view (click count) of class type courses today up to now which is contributed by search engines.
      */
    cleanData.map(x => {
                  // potential referers: - or http://cn.bing.com/search?q=Big Data Interview Skills
                  val referer = x.referer.replace("//", "/")
                  val splits = referer.split("/")
                  var searchEngine = ""
                  if (splits.length > 2) {  // for this kind of referer: http://cn.bing.com/search?q=Big Data Interview Skills
                    searchEngine = splits(1)
                  }
                  (x.time.substring(0, 8), searchEngine, x.courseId)})
              .filter(_._2 != "")
              .map(x => {(x._1 + "_" + x._2 + "_" + x._3, 1)})  // get (day_searchEngine_courseId, 1) pairs
              .reduceByKey(_+_)
              .foreachRDD(rdd => {rdd.foreachPartition(partitionRecords => {
                  val list = new ListBuffer[ClassTypeCourseSearchClickCount]
                  partitionRecords.foreach(pair => {
                    list.append(ClassTypeCourseSearchClickCount(pair._1, pair._2))
                  })

                ClassTypeCourseSearchClickCountDao.save(list)
                })
              })

    ssc.start()
    ssc.awaitTermination()
  }
}
