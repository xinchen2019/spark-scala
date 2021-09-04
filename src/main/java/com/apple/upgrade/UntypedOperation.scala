package com.apple.upgrade

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * @Program: spark-scala
  * @ClassName: UntypedOperation
  * @Description: TODO
  * @Author Mr.Apple
  * @Create: 2021-09-04 23:30
  * @Version 1.1.0
  **/
object UntypedOperation {
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
    import spark.implicits._
    val employeeDF = spark.read.json("data\\employee.json")
    val departmentDF = spark.read.json("data\\department.json")

    employeeDF.where("age>20")
      .join(departmentDF, $"depId" === $"id")
      .groupBy(departmentDF("name"), employeeDF("gender"))
      .agg(avg(employeeDF("salary")))
      .show()

    employeeDF.select($"name", $"depId", $"salary")
      .where("age>30")
      .show()
  }
}
