package com.apple.atguigu.structured.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{LongType, StringType, StructType}

/**
  * @Program: spark-scala
  * @ClassName: BasicOperation
  * @Description: TODO
  * @Author Mr.Apple
  * @Create: 2021-09-06 15:33
  * @Version 1.1.0
  **/
object UnTypeOpt {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("UnTypeOpt")
      .getOrCreate()

    val peopleSchema = new StructType()
      .add("name", StringType)
      .add("age", LongType)
      .add("sex", StringType)

    val peopleDF = spark.readStream
      .schema(peopleSchema)
      .json("data//json//pp") //等价于 format("json").load(path)

    val df = peopleDF.select("name", "age", "sex")
      .where("age>20") //弱类型 api

    df.writeStream
      .outputMode("append")
      .format("console")
      .start()
      .awaitTermination()

  }
}
