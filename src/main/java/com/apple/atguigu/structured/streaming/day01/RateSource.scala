package com.apple.atguigu.structured.streaming.day01

import org.apache.spark.sql.SparkSession

/**
  * @Program: spark-scala
  * @ClassName: RateSource
  * @Description: TODO
  * @Author Mr.Apple
  * @Create: 2021-09-06 15:19
  * @Version 1.1.0
  **/
object RateSource {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("RateSource")
      .getOrCreate()

    val df = spark.readStream
      .format("rate")
      .option("rowsPerSecond", 10)
      .option("rampUpTime", 1)
      .option("numPartitions", 3)
      .load()

    df.writeStream
      .format("console")
      .outputMode("update")
      .option("truncate", false)
      .start()
      .awaitTermination()

  }
}
