package com.zhandev.spark.project.domain

/**
  * Class type course click count which is contributed by search engines bean.
  * @param day_searchEngine_courseId  corresponds to rowkey in the HBase table, e.g. 20180101_www.google.com_123
  * @param click_count  corresponds to click count of the class type course which is contributed by search engines (namely, the click count of course 123 in 20180101 guided by search engines)
  */
case class ClassTypeCourseSearchClickCount(day_searchEngine_courseId:String, click_count:Long)
