package com.zhandev.spark.project.dao

import com.zhandev.spark.project.domain.ClassTypeCourseClickCount

import scala.collection.mutable.ListBuffer

/**
  *  Data access layer of class type course click count
  */
object ClassTypeCourseClickCountDao {

  val tableName = "imooc_course_clickcount"
  val cloumnFamily = "info"
  val qualifer = "click_count"


  /**
    * Save data into HBase
    * Save data in batch rather than one by one
    * @param list  ClassTypeCourseClickCount list
    */
  def save(list: ListBuffer[ClassTypeCourseClickCount]): Unit = {

//    val table = HBaseUtils.getInstance().getTable(tableName)
//
//    for(ele <- list) {
//      table.incrementColumnValue(Bytes.toBytes(ele.day_course),
//        Bytes.toBytes(cf),
//        Bytes.toBytes(qualifer),
//        ele.click_count)
//    }

  }


  /**
    * Get click count by rowkey (day_courseId)
    */
  def getClickCount(day_courseId: String): Long = {
//    val table = HBaseUtils.getInstance().getTable(tableName)
//
//    val get = new Get(Bytes.toBytes(day_course))
//    val value = table.get(get).getValue(cf.getBytes, qualifer.getBytes)
//
//    if(value == null) {
//      0L
//    }else{
//      Bytes.toLong(value)
//    }
    0l
  }

//  def main(args: Array[String]): Unit = {
//
//
//    val list = new ListBuffer[CourseClickCount]
//    list.append(CourseClickCount("20171111_8",8))
//    list.append(CourseClickCount("20171111_9",9))
//    list.append(CourseClickCount("20171111_1",100))
//
//    save(list)
//
//    println(count("20171111_8") + " : " + count("20171111_9")+ " : " + count("20171111_1"))
//  }
}
