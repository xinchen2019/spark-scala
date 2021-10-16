package com.apple.atguigu.structured.streaming.day02

import java.sql.Timestamp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

/**
  * @Program: spark-scala
  * @ClassName: steamingTosteaming
  * @Description: TODO
  * @Author Mr.Apple
  * @Create: 2021-09-07 20:45
  * @Version 1.1.0
  **/

/**
  * lisi,female,2019-09-16 11:50:00
  * zs,male,2019-09-16 11:51:00
  * ww,female,2019-09-16 11:52:00
  * zhiling,female,2019-09-16 11:53:00
  * fengjie,female,2019-09-16 11:54:00
  * yifei,female,2019-09-16 11:55:00
  */

/**
  * lisi,18,2019-09-16 11:50:00
  * zs,19,2019-09-16 11:51:00
  * ww,20,2019-09-16 11:52:00
  * zhiling,22,2019-09-16 11:53:00
  * fengjie,30,2019-09-16 11:54:00
  * yifei,98,2019-09-16 11:55:00
  */
object steamsteamJoin {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("steamsteamJoin")
      .getOrCreate()

    import spark.implicits._

    val nameSexStream = spark.readStream
      .format("socket")
      .option("host", "master")
      .option("port", "9998")
      .load()
      .as[String]
      .map { line =>
        val arr = line.split(",")
        (arr(0), arr(1), Timestamp.valueOf(arr(2)))
      }.toDF("name", "sex", "ts1")

    val nameAgeStream = spark.readStream
      .format("socket")
      .option("host", "master")
      .option("port", "9999")
      .load()
      .as[String]
      .map { line =>
        val arr = line.split(",")
        (arr(0), arr(1), Timestamp.valueOf(arr(2)))
      }.toDF("name", "sex", "ts2")

    val joinResult = nameSexStream.join(nameAgeStream, "name")

    joinResult.writeStream
      .outputMode("append")
      .format("console")
      .trigger(Trigger.ProcessingTime(0))
      .start()
      .awaitTermination()
  }
}
