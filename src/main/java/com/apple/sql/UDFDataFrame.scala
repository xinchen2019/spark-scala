package com.apple.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * @Program: spark-scala
  * @ClassName: UDFDataFrame
  * @Description: TODO
  * @Author Mr.Apple
  * @Create: 2021-10-16 18:23
  * @Version 1.1.0
  **/
object UDFDataFrame {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("UDFDataFrame")
      .getOrCreate()
    val userData = Array(
      ("A", 16),
      ("B", 21),
      ("B", 14),
      ("B", 18)
    )
    val userDF = spark.createDataFrame(userData).toDF("name", "age")
    val strLen = udf((str: String) => str.length())
    val udf_isAdult = udf(isAdult _)

    userDF.withColumn("name_len", strLen(col("name"))).withColumn("isAdult", udf_isAdult(col("age"))).show

    //通过select添加列
    userDF.select(col("*"), strLen(col("name")) as "name_len", udf_isAdult(col("age")) as "isAdult").show
  }

  def isAdult(age: Int): Boolean = {
    if (age < 18) {
      false
    } else {
      true
    }
  }
}
