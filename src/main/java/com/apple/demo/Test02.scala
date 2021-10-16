package com.apple.demo

import java.text.SimpleDateFormat

/**
  * @Program: spark-scala
  * @ClassName: Test02
  * @Description: TODO
  * @Author Mr.Apple
  * @Create: 2021-09-06 17:22
  * @Version 1.1.0
  **/
object Test02 {
  def main(args: Array[String]): Unit = {

    val windowDuration: Double = 10
    val slideDuration: Double = 5.0
    var maxNumOverlapping: Double = scala.math.ceil(windowDuration / slideDuration)
    //2019-09-25 12:07:00,dog
    val eventTime = "2019-09-25 12:07:00"
    val timestamp = dateToTimestamp(eventTime)
    val startTime = 0
    println(maxNumOverlapping)
    //    for (i <- 0 until maxNumOverlapping){
    //       windowId <- ceil((timestamp - startTime) / slideDuration)
    //      windowStart <- windowId * slideDuration + (i - maxNumOverlapping) * slideDuration + startTime
    //       windowEnd <- windowStart + windowDuration
    //       return windowStart, windowEnd

    //    for (i <- 0 until maxNumOverlapping) {
    //
    //      var windowId = scala.math.ceil((timestamp - startTime) / slideDuration)
    //      var windowStart = windowId * slideDuration + (i - maxNumOverlapping) * slideDuration + startTime
    //      var windowEnd = windowStart + windowDuration
    //      println("windowStart: " + windowStart + "windowEnd: " + windowEnd)
    //    }
  }

  def dateToTimestamp(dtString: String): Long = {
    //2019-09-25 12:07:00
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val dt = sdf.parse(dtString);
    val timestamp = dt.getTime();
    return timestamp;
  }
}
