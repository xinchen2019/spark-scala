package com.apple.atguigu.structured.streaming.day02

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger


/**
  * @Program: spark-scala
  * @ClassName: Window1
  * @Description: TODO
  * @Author Mr.Apple
  * @Create: 2021-09-07 10:00
  * @Version 1.1.0
  **/

/** nc -lk 9999
  * 测试数据:
  * 2019-09-25 09:50:25,hello
  */

/**
  * 参考链接
  * https://blog.csdn.net/xiaoweite1/article/details/116040080
  */
object Window2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Window2")
      .getOrCreate()
    import org.apache.spark.sql.functions._
    import spark.implicits._
    spark.readStream
      .format("socket")
      .option("host", "master")
      .option("port", "9999")
      .load()
      .as[String]
      .map(line => {
        val splited = line.split(",")
        (splited(0), splited(1))
      }).toDF("ts", "word")
      .groupBy(
        window($"ts",
          "10 minutes",
          "5 minutes"),
        $"word"
      ).count()
      .sort("window")
      .writeStream
      .format("console")
      .outputMode("complete")
      .trigger(Trigger.ProcessingTime(2000))
      .option("truncate", false)
      .start()
      .awaitTermination()
  }
}
