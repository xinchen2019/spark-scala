package com.apple.atguigu.structured.streaming.day02

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

/**
  * @Program: spark-scala
  * @ClassName: KafkaSink
  * @Description: 以 Streaming 方式输出数据(这种方式使用流的方式源源不断的向 kafka 写入数据.)
  * @Author Mr.Apple
  * @Create: 2021-09-07 22:20
  * @Version 1.1.0
  **/
object KafkaSink {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("KafkaSink")
      .getOrCreate()
    import spark.implicits._

    val lines = spark.readStream
      .format("socket")
      .option("host", "master")
      .option("port", "9999")
      .load()

    val words = lines.as[String].flatMap(_.split("\\W+"))
      .groupBy("value")
      .count()
      .map(row => row.getString(0) + "," + row.getLong(1))
      .toDF("value")

    words.writeStream
      .outputMode("update")
      .format("kafka")
      .trigger(Trigger.ProcessingTime(0))
      .option("kafka.bootstrap.servers", "master:9092,slave1:9092,slave2:9092")
      .option("topic", "update")
      .option("checkpointLocation", "checkpoint")
      .start()
      .awaitTermination()
  }
}
