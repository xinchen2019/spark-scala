package com.apple.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @Program: spark-scala
  * @ClassName: WordCount
  * @Description: TODO
  * @Author Mr.Apple
  * @Create: 2021-08-16 17:20
  * @Version 1.1.0
  **/
object WordCount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("WordCount")

    val ssc = new StreamingContext(conf, Seconds(1))
    ssc.sparkContext.setLogLevel("ERROR")
    val lines = ssc.socketTextStream("master", 9999)
    val words = lines.flatMap {
      _.split(" ")
    }
    val pairs = words.map { word => (word, 1) }
    val wordCounts = pairs.reduceByKey(_ + _)

    Thread.sleep(5000)
    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
