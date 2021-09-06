package com.apple.atguigu.structured.streaming


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}


/**
  * @Program: spark-scala
  * @ClassName: FileSource
  * @Description: TODO
  * @Author Mr.Apple
  * @Create: 2021-09-06 11:54
  * @Version 1.1.0
  **/
/**
  * 参考链接
  * http://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html
  */

object FileSource {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("FileSource")
      .getOrCreate()

    val userSchema = StructType(
      StructField("name", StringType) ::
        StructField("age", IntegerType) ::
        StructField("sex", StringType) :: Nil
    )
    val df = spark.readStream
      .format("csv")
      .schema(userSchema)
      .load("data\\csv\\testcsv")

    df.writeStream
      .format("console")
      .outputMode("update")
      .trigger(Trigger.ProcessingTime(1000))
      .start()
      .awaitTermination()
  }
}
