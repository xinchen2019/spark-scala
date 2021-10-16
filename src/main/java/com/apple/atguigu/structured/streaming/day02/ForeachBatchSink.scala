package com.apple.atguigu.structured.streaming.day02

import java.util.Properties

import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @Program: spark-scala
  * @ClassName: ForeachBatchSink
  * @Description: TODO
  * @Author Mr.Apple
  * @Create: 2021-09-08 09:10
  * @Version 1.1.0
  **/
object ForeachBatchSink {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("ForeachBatchSink")
      .getOrCreate()
    import spark.implicits._

    val lines: DataFrame = spark.readStream
      .format("socket") // 设置数据源
      .option("host", "master")
      .option("port", 9999)
      .load

    val wordCount: DataFrame = lines.as[String]
      .flatMap(_.split("\\W+"))
      .groupBy("value")
      .count()

    val props = new Properties()
    props.setProperty("user", "ubuntu")
    props.setProperty("password", "123456")

    val query: StreamingQuery = wordCount.writeStream
      .outputMode("complete")
      .foreachBatch((df, batchId) => { // 当前分区id, 当前批次id
        println("batchId: " + batchId)
        if (df.count() != 0) {
          df.cache()
          df.write.json(s"./$batchId")
          df.write.mode("overwrite")
            .jdbc("jdbc:mysql://master:3306/apple?useSSL=false&createDatabaseIfNotExist=true&useUnicode=true&characterEncoding=UTF-8", "word_count", props)
        }
      })
      .start()
    query.awaitTermination()
  }
}