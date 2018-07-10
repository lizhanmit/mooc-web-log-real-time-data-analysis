package com.zhandev.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Spark Streaming processes file system (local/hdfs) data
  */
object FileWordCount {

  def main(args: Array[String]): Unit = {

    // can use "local" here as there is no receiver for processing file system data, but it is not recommended
    val sparkConf = new SparkConf().setMaster("local").setAppName("FileWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val lines = ssc.textFileStream("file:///home/hadoop/IdeaProjects/sparktrain/src/main/resources/static/file")

    val result = lines.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
    result.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
