package com.apple.atguigu.structured.streaming.day02

import java.util.{Timer, TimerTask}

import org.apache.spark.sql.SparkSession

/**
  * @Program: spark-scala
  * @ClassName: MemorySink
  * @Description: TODO
  * @Author Mr.Apple
  * @Create: 2021-09-07 23:40
  * @Version 1.1.0
  **/
object MemorySink {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("MemorySink")
      .getOrCreate()
    import spark.implicits._

    val lines = spark.readStream
      .format("socket")
      .option("host", "master")
      .option("port", "9999")
      .load()

    val words = lines.as[String]
      .flatMap(_.split("\\W++"))
      .groupBy("value")
      .count()

    val query = words.writeStream
      .outputMode("complete")
      .format("memory") //memory sink
      .queryName("word_count") //内存临时表名
      .start()

    val timer = new Timer
    val task = new TimerTask {
      override def run(): Unit = {
        spark.sql("select * from word_count").show()
      }
    }
    timer.schedule(task, 0, 2000)
    query.awaitTermination()
  }
}
