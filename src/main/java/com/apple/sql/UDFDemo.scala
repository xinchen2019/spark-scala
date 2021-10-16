package com.apple.sql

import org.apache.spark.sql.SparkSession

/**
  * @Program: spark-scala
  * @ClassName: UDFDemo
  * @Description: TODO
  * @Author Mr.Apple
  * @Create: 2021-10-16 17:45
  * @Version 1.1.0
  *          参考链接 https://www.cnblogs.com/yyy-blog/p/10280657.html
  **/
object UDFDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("UDFDemo")
      .getOrCreate()
    val userData = Array(
      ("A", 16),
      ("B", 21),
      ("B", 14),
      ("B", 18)
    )
    val userDF = spark.createDataFrame(userData).toDF("name", "age")

    userDF.show()
    userDF.createOrReplaceTempView("user")

    spark.udf.register("isAdult", isAdult _)
    //匿名函数注册UDF
    spark.udf.register("strLen", (str: String) => str.length)
    //spark.sql("select name,strLen(name) as name_len from user").show()
    spark.sql("select name,strLen(name) as name_len,isAdult(age) as isAdult from user").show()
  }

  def isAdult(age: Int): Boolean = {
    if (age < 18) {
      false
    } else {
      true
    }
  }
}
