package com.apple.atguigu.structured.streaming.day02

import org.apache.spark.sql.SparkSession

/**
  * @Program: spark-scala
  * @ClassName: KafkaSink
  * @Description: 以 batch 方式输出数据(这种方式输出离线处理的结果, 将已存在的数据分为若干批次进行处理. 处理完毕后程序退出.)
  * @Author Mr.Apple
  * @Create: 2021-09-07 22:20
  * @Version 1.1.0
  **/
object KafkaSinkBatch {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("KafkaSinkBatch")
      .getOrCreate()
    import spark.implicits._

    val wordDF = spark.createDataset(
      Array("hello word", "hello you", "hello me"))


    val words = wordDF.flatMap(_.split("\\W+"))
      .toDF("word")
      .groupBy("word")
      .count()
      .map(row => row.getString(0) + "," + row.getLong(1))
      .toDF("value")

    words.write
      .format("kafka")
      .option("kafka.bootstrap.servers", "master:9092,slave1:9092,slave2:9092")
      .option("topic", "update")
      .save()
  }
}
