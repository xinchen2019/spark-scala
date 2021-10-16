package com.apple.atguigu.structured.streaming.day01

import org.apache.spark.sql.SparkSession

/**
  * @Program: spark-scala
  * @ClassName: KafkaSource
  * @Description: TODO
  * @Author Mr.Apple
  * @Create: 2021-09-06 12:21
  * @Version 1.1.0
  **/
object KafkaSource {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("KafkaSource")
      .getOrCreate()

    import spark.implicits._
    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "master:9092,slave1:9092,slave2:9092")
      .option("subscribe", "test")
      .load()
      //.select("value")
      .selectExpr("cast(value as string)")
      .as[String]
      .flatMap(_.split(" "))
      .groupBy("value")
      .count()

    df.writeStream
      .format("console")
      .outputMode("update")
      .start()
      .awaitTermination()
  }
}
