package com.apple.atguigu.structured.streaming.day01

import java.sql.Timestamp

import org.apache.spark.sql.SparkSession

/**
  * @Program: spark-scala
  * @ClassName: Window1
  * @Description: TODO
  * @Author Mr.Apple
  * @Create: 2021-09-06 16:18
  * @Version 1.1.0
  **/
object Window1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Window1")
      .getOrCreate()
    import org.apache.spark.sql.functions._
    import spark.implicits._
    val lines = spark.readStream
      .format("socket")
      .option("host", "master")
      .option("port", "9999")
      .option("includeTimestamp", true) //给产生的数据自动添加时间戳
      .load()
      .as[(String, Timestamp)]
      .flatMap {
        case (words, ts) => words.split("\\W+").map((_, ts))
      }
      .toDF("word", "ts")
      .groupBy(
        window($"ts",
          "10 minutes",
          "5 minutes"),
        $"word"
      ).count()

    lines.writeStream
      .format("console")
      .outputMode("update")
      .option("truncate", false)
      .start()
      .awaitTermination()

  }
}
