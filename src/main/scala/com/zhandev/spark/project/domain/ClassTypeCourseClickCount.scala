package com.zhandev.spark.project.domain

/**
  * Class type course click count bean
  * @param day_courseId  corresponds to rowkey in the HBase table, e.g. 20180101_123
  * @param click_count  corresponds to click count of the class type course (namely, the click count of course 123 in 20180101)
  */
case class ClassTypeCourseClickCount(day_courseId:String, click_count:Long)
