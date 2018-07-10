package com.zhandev.spark

import java.sql.DriverManager

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Spark Streaming processes socket data and save the wordcount result into MySQL
  */
object WordCountResultToMysql {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("WordCountResultToMysql").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val lines = ssc.socketTextStream("localhost", 6789)

    val result = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    result.print()

    result.foreachRDD(rdd => {
      rdd.foreachPartition(partitionOfRecords => {
        val connection = createConnection()
        partitionOfRecords.foreach(record => {
          val sql = "insert into wordcount(word, count) values('" + record._1 + "'," + record._2 + ")"
          connection.createStatement().execute(sql)
        })

        connection.close()
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }


  /**
    * Create MySQL connection
    */
  def createConnection() = {
    Class.forName("com.mysql.jdbc.Driver")
    DriverManager.getConnection("jdbc:mysql://localhost:3306/spark", "root", "admin")
  }
}
