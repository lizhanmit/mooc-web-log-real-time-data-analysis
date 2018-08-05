package com.zhandev.spark.project.domain

/**
  * Web log info after data cleansing.
  * @param ip  access ip address of the log
  * @param time  access time of the log
  * @param courseId  access courseId of the class type course of the log
  * @param statusCode access status code of the log
  * @param referer  access referer of the log
  */
case class ClickLog(ip:String, time:String, courseId:Int, statusCode:Int, referer:String)
