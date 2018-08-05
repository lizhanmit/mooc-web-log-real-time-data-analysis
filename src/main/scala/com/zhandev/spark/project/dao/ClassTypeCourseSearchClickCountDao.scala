package com.zhandev.spark.project.dao

import com.zhandev.spark.project.domain.ClassTypeCourseSearchClickCount
import com.zhandev.spark.project.utils.HBaseUtils
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer

/**
  * Data access layer of class type course click count which is contributed by search engines.
  */
object ClassTypeCourseSearchClickCountDao {

  val tableName = "mooc_course_search_clickcount"
  val cloumnFamily = "info"
  val qualifer = "click_count"


  /**
    * Save data into HBase.
    * Save data in batch rather than one by one.
    * @param list  ClassTypeCourseSearchClickCount list
    */
  def save(list: ListBuffer[ClassTypeCourseSearchClickCount]): Unit = {

    val table = HBaseUtils.getInstance().getTable(tableName)

    for(ele <- list) {
      table.incrementColumnValue(Bytes.toBytes(ele.day_searchEngine_courseId),
        Bytes.toBytes(cloumnFamily),
        Bytes.toBytes(qualifer),
        ele.click_count)
    }

  }


  /**
    * Get click count by rowkey (day_searchEngine_courseId).
    */
  def getClickCount(day_searchEngine_courseId: String): Long = {
    val table = HBaseUtils.getInstance().getTable(tableName)

    val get = new Get(Bytes.toBytes(day_searchEngine_courseId))
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
    val list = new ListBuffer[ClassTypeCourseSearchClickCount]
    list.append(ClassTypeCourseSearchClickCount("20180804_www.google.com_100", 5))
    list.append(ClassTypeCourseSearchClickCount("20180804_www.google.com_200", 10))
    list.append(ClassTypeCourseSearchClickCount("20180804_www.google.com_300", 15))

    save(list)

    println("20180804_www.google.com_100: " + getClickCount("20180804_www.google.com_100"))
    println("20180804_www.google.com_200: " + getClickCount("20180804_www.google.com_200"))
    println("20180804_www.google.com_300: " + getClickCount("20180804_www.google.com_300"))
  }
}
