package com.apple.atguigu.structured.streaming.day02

import org.apache.spark.sql.SparkSession

/**
  * @Program: spark-scala
  * @ClassName: FileSink
  * @Description: TODO
  * @Author Mr.Apple
  * @Create: 2021-09-07 22:01
  * @Version 1.1.0
  **/
object FileSink {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("FileSink")
      .getOrCreate()
    import spark.implicits._

    val lines = spark.readStream
      .format("socket")
      .option("host", "master")
      .option("port", "9999")
      .load()

    val words = lines.as[String].flatMap {
      line => line.split("\\w+").map(word => (word, word.reverse))
    }.toDF("orginWord", "reverseWord")

    words.writeStream.outputMode("append")
      .format("json") //支持 "orc","json","csv"
      .option("path", "dataOut/")
      .option("checkpointLocation", "checkpoint")
      .start()
      .awaitTermination()

  }
}
