package com.zhandev.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Spark Streaming processes socket data (wordcount)
  *
  * Testing: nc -lk 6789
  */
object NetworkWordCount {

  def main(args: Array[String]): Unit = {

    // do not use "local" or "local[1]", as the receiver has already taken up one thread
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")

    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val lines = ssc.socketTextStream("localhost", 6789)

    val result = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    result.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
