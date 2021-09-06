package com.apple.upgrade

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * @Program: spark-scala
  * @ClassName: OtherFunction
  * @Description: TODO
  * @Author Mr.Apple
  * @Create: 2021-09-05 08:43
  * @Version 1.1.0
  **/
object OtherFunction {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder() //用到了java里面的构造设计模式
      .appName("UntypedOperation")
      .master("local[*]")
      //这是Spark SQL 2.0 里面一个重要的变化，需要设置spark sql的元数据仓库的目录
      //.config("spark.sql.warehouse.dir", warehouseLocation)
      //启用hive支持
      .getOrCreate()
    import org.apache.spark.sql.functions._
    val employeeDF = spark.read.json("data\\employee.json")
    val departmentDF = spark.read.json("data\\department.json")

    /**
      * 日期函数：current_date、current_timestamp
      * 数学函数：round
      * 随机函数：rand
      * 字符串函数：concat、concat_ws
      * 自定义udf和udaf函数
      */
    employeeDF.select(employeeDF("name"),
      current_date(),
      current_timestamp(),
      rand(),
      round(employeeDF("salary"), 2),
      concat(employeeDF("gender"), employeeDF("age"))
      , concat_ws("|", employeeDF("gender"), employeeDF("age")))
      .show()
  }
}
