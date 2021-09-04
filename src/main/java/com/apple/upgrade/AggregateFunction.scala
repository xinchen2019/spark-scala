package com.apple.upgrade

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * @Program: spark-scala
  * @ClassName: AggregateFunction
  * @Description: TODO
  * @Author Mr.Apple
  * @Create: 2021-09-05 00:00
  * @Version 1.1.0
  **/
object AggregateFunction {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder() //用到了java里面的构造设计模式
      .appName("AggregateFunction")
      .master("local[*]")
      //这是Spark SQL 2.0 里面一个重要的变化，需要设置spark sql的元数据仓库的目录
      //.config("spark.sql.warehouse.dir", warehouseLocation)
      //启用hive支持
      .getOrCreate()
    import org.apache.spark.sql.functions._
    val employeeDF = spark.read.json("data\\employee.json")
    val departmentDF = spark.read.json("data\\department.json")

    //    employeeDF
    //      .join(departmentDF, $"depId" === $"id")
    //      .groupBy(departmentDF("name"))
    //      .agg(avg(employeeDF("salary")), sum(employeeDF("salary")), max(employeeDF("salary")), min(employeeDF("salary")), count(employeeDF("name")), countDistinct(employeeDF("name")))
    //      .show()

    /**
      * collect_list和collect_set，都用于将同一个分组内的指定字段的值串起来，变成一个数组
      * 常用于行转列
      * 比如说
      * depId=1,employee=leo
      * depId=1,employee=jack
      * depId=1,employees=[leo,jack]
      */
    employeeDF
      .groupBy(employeeDF("depId"))
      .agg(collect_set(employeeDF("name")), collect_list(employeeDF("name")))
      .collect()
      .foreach(println(_))
  }
}
