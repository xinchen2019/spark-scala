package com.apple.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @Program: spark-scala
  * @ClassName: WindowHotWord
  * @Description: TODO
  * @Author Mr.Apple
  * @Create: 2021-08-24 10:47
  * @Version 1.1.0
  **/
object WindowHotWord {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]")
      .setAppName("WindowHotWord")

    val ssc = new StreamingContext(conf, Seconds(1))

    val seacrhLogsDStream = ssc.socketTextStream("master", 9999)
    val searchWordsDStream = seacrhLogsDStream.map((_.split(" ")(1)))
    val searchWordPairsDStream = searchWordsDStream.map((_, 1))

    val searchWordCountsDSteram = searchWordPairsDStream
      .reduceByKeyAndWindow(
        (v1: Int, v2: Int) => v1 + v2,
        Seconds(60),
        Seconds(10))

    searchWordCountsDSteram.transform(
      searchWordCountsRDD => {
        val countSearchWordsRDD = searchWordCountsRDD.map(tuple => (tuple._2, tuple._1))

        searchWordCountsRDD
      })

  }
}
