package com.apple.sql

import org.apache.spark.sql.SparkSession

/**
  * @Program: spark-scala
  * @ClassName: UdfDemo1
  * @Description: TODO
  * @Author Mr.Apple
  * @Create: 2021-10-16 18:10
  * @Version 1.1.0
  **/
object UdfDemo1 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("newUdf")
      .master("local[*]")
      .getOrCreate()

    // 构造测试数据，有两个字段、名字和年龄
    val userData = Array(("Leo", 16), ("Marry", 21), ("Jack", 14), ("Tom", 18))

    //创建测试df
    val userDF = spark.createDataFrame(userData).toDF("name", "age")

    // 注册一张user表
    userDF.createOrReplaceTempView("user")

    //注册自定义函数（通过匿名函数）
    spark.udf.register("strLen", (str: String) => str.length())
    spark.udf.register("NameAddAge", (x: String, y: Int) => x + ":" + y)

    //注册自定义函数（通过实名函数）
    spark.udf.register("isAdult", isAdult _)
    spark.udf.register("isLength", isLength _)
    spark.udf.register("con", con(_: String, _: Int))
    spark.sql("select *," +
      "isLength(name) as name_len_shi," +
      "strLen(name) as name_len_ni," +
      "NameAddAge(name,age) as NameAge," +
      "con(name,age) as AgeName," +
      "isAdult(age) as isAdult from user").show

    //关闭
    spark.stop()
  }

  def isAdult(age: Int) = {
    if (age < 18) {
      false
    } else {
      true
    }
  }

  /**
    * 根据名字计算姓名的的长度
    */
  def isLength(name: String) = {
    name.length
  }

  def con(name: String, age: Int) = {
    age + ":" + name
  }

  def con1(name: String, age: Int) = {
    name + ":" + age
  }
}
