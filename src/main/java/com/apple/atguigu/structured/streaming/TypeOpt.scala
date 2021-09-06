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
case class Ppeople(name: String, age: Long, sex: String)

object TypeOpt {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("TypeOpt")
      .getOrCreate()
    import spark.implicits._
    val peopleSchema = new StructType()
      .add("name", StringType)
      .add("age", LongType)
      .add("sex", StringType)

    val peopleDF = spark.readStream
      .schema(peopleSchema)
      .json("data//json//pp") //等价于 format("json").load(path)

    val ds = peopleDF.as[Ppeople].filter(_.age > 20).map(_.name)


    ds.writeStream
      .outputMode("append")
      .format("console")
      .start()
      .awaitTermination()

  }
}
