package com.apple.atguigu.structured.streaming.day02


import java.sql.Timestamp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.window
import org.apache.spark.sql.streaming.Trigger


/**
  * @Program: spark-scala
  * @ClassName: Watemark1
  * @Description: TODO
  * @Author Mr.Apple
  * @Create: 2021-09-07 18:51
  * @Version 1.1.0
  **/
object Watemark1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Watemark1")
      .getOrCreate()

    import spark.implicits._
    val lines = spark.readStream
      .format("socket")
      .option("host", "master")
      .option("port", "9999")
      .load()

    val words = lines
      .as[String].flatMap(line => {
      val split = line.split(",")
      split(1).split(" ").map((_, Timestamp.valueOf(split(0))))
    }).toDF("word", "timestamp")

    val wordCounts = words
      .withWatermark("timestamp", "2 minutes")
      .groupBy(window($"timestamp",
        "10 minutes",
        "2 minutes"),
        $"word")
      .count()
      .sort("window")

    val query = wordCounts.writeStream
      .outputMode("complete")
      .trigger(Trigger.ProcessingTime(1000))
      .format("console")
      .option("truncate", "false")
      .start()

    query.awaitTermination()
  }
}
