package com.apple.upgrade

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

/**
  * @Program: spark-scala
  * @ClassName: StructuredSocket
  * @Description: TODO
  * @Author Mr.Apple
  * @Create: 2021-09-05 12:57
  * @Version 1.1.0
  **/
object StructuredSocket {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("StructuredSocket")
      .master("local[*]")
      .getOrCreate()
    //    val socketDF = spark
    //      .readStream
    //      .format("socket")
    //      .option("host", "localhost")
    //      .option("port", 9999)
    //      .load()
    //    println(socketDF.isStreaming)
    //    println("===================")
    //    socketDF.printSchema

    val userSchema = new StructType()
      .add("name", "string")
      .add("age", "integer")
    val userDF = spark
      .readStream
      .option("sep", ",")
      .schema(userSchema)
      .csv("data\\csv\\") //必须是目录 不是文件名

    val query = userDF.writeStream
      //.outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()
  }
}
