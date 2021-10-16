package com.apple.atguigu.structured.streaming.day02


import java.sql.Timestamp

import org.apache.spark.sql.SparkSession

/**
  * @Program: spark-scala
  * @ClassName: DropDumplicate1
  * @Description: TODO
  * @Author Mr.Apple
  * @Create: 2021-09-07 19:36
  * @Version 1.1.0
  **/

/**
  * 1,2019-09-14 11:50:00,dog
  * 2,2019-09-14 11:51:00,dog
  * 1,2019-09-14 11:50:00,dog
  * 3,2019-09-14 11:53:00,dog
  * 1,2019-09-14 11:50:00,dog
  * 4,2019-09-14 11:45:00,dog
  */
object DropDumplicate1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("")
      .getOrCreate()
    import spark.implicits._
    val lines = spark.readStream
      .format("socket")
      .option("host", "master")
      .option("port", "9999")
      .load()
    val words = lines.as[String].map(line => {
      var arr = line.split(",")
      (arr(0), Timestamp.valueOf(arr(1)), arr(2))
    }).toDF("uid", "ts", "word")

    val wordCount = words
      .withWatermark("ts", "2 minutes")
      .dropDuplicates("uid") //去重重复数据 uid相同就是重复，可以传递多个列

    wordCount.writeStream
      .outputMode("append")
      .format("console")
      .start()
      .awaitTermination()
  }
}
