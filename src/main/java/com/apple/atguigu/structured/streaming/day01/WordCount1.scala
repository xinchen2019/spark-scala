package com.apple.atguigu.structured.streaming.day01

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

/**
  * @Program: spark-scala
  * @ClassName: WordCount1
  * @Description: TODO
  * @Author Mr.Apple
  * @Create: 2021-09-05 15:45
  * @Version 1.1.0
  **/
object WordCount1 {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("WorldCount1")
      .getOrCreate()

    val lines = spark.readStream
      .format("socket")
      .option("host", "master")
      .option("port", 9999)
      .load()
    import spark.implicits._

    //lines.as[String].flatMap(_.split(" ")).groupBy("value").count()
    lines.as[String].flatMap(_.split(" ")).createOrReplaceTempView("w")
    val wordCount = spark.sql(
      """
        |select
        | *,
        | count(*) count
        |from w
        |group by value
      """.stripMargin)
    println("===================================")
    val result = wordCount.writeStream
      .format("console")
      .outputMode("complete") //complete、update
      .trigger(Trigger.ProcessingTime("2 seconds"))
      .start()

    result.awaitTermination()
    spark.stop()
  }
}
