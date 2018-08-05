package com.zhandev.spark.project.dao

import com.zhandev.spark.project.domain.ClassTypeCourseClickCount
import com.zhandev.spark.project.utils.HBaseUtils
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer

/**
  * Data access layer of class type course click count.
  */
object ClassTypeCourseClickCountDao {

  val tableName = "mooc_course_clickcount"
  val cloumnFamily = "info"
  val qualifer = "click_count"


  /**
    * Save data into HBase.
    * Save data in batch rather than one by one.
    * @param list  ClassTypeCourseClickCount list
    */
  def save(list: ListBuffer[ClassTypeCourseClickCount]): Unit = {

    val table = HBaseUtils.getInstance().getTable(tableName)

    for(ele <- list) {
      table.incrementColumnValue(Bytes.toBytes(ele.day_courseId),
        Bytes.toBytes(cloumnFamily),
        Bytes.toBytes(qualifer),
        ele.click_count)
    }

  }


  /**
    * Get click count by rowkey (day_courseId).
    */
  def getClickCount(day_courseId: String): Long = {
    val table = HBaseUtils.getInstance().getTable(tableName)

    val get = new Get(Bytes.toBytes(day_courseId))
    val value = table.get(get).getValue(cloumnFamily.getBytes, qualifer.getBytes)

    if(value == null) {
      0L
    }else{
      Bytes.toLong(value)
    }
  }

  /**
    * For testing functions.
    */
  def main(args: Array[String]): Unit = {
    val list = new ListBuffer[ClassTypeCourseClickCount]
    list.append(ClassTypeCourseClickCount("20180804_100", 5))
    list.append(ClassTypeCourseClickCount("20180804_200", 10))
    list.append(ClassTypeCourseClickCount("20180804_300", 15))

    save(list)

    println("20180804_100: " + getClickCount("20180804_100"))
    println("20180804_200: " + getClickCount("20180804_200"))
    println("20180804_300: " + getClickCount("20180804_300"))
  }
}
