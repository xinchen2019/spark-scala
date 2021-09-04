package com.apple.upgrade

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * @Program: spark-scala
  * @ClassName: DepartmentAvgSalaryAndAgeStat
  * @Description: TODO
  * @Author Mr.Apple
  * @Create: 2021-09-03 21:10
  * @Version 1.1.0
  **/

/**
  * 计算部门的平均薪资和年龄
  *
  * 需求：
  * 1、只统计年龄在20岁以上的员工
  * 2、根据部门名称和员工性别为粒度来进行统计
  * 3、统计出每个部门分性别的平均薪资和年龄
  *
  */
object DepartmentAvgSalaryAndAgeStat {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {


    val spark = SparkSession
      .builder() //用到了java里面的构造设计模式
      .appName("SparkSQLDemo")
      .master("local[*]")
      //这是Spark SQL 2.0 里面一个重要的变化，需要设置spark sql的元数据仓库的目录
      //.config("spark.sql.warehouse.dir", warehouseLocation)
      //启用hive支持
      .enableHiveSupport()
      .getOrCreate()
    // 导入spark的隐式转换
    import spark.implicits._
    // 导入spark sql的functions
    import org.apache.spark.sql.functions._

    val employeeDF = spark.read.json("data/employee.json")
    val departmentDF = spark.read.json("data/department.json")
//    employeeDF.filter($"age" > 20)
//      .join(departmentDF, $"depId" === $"id")
//      .groupBy(departmentDF("name"), employeeDF("gender"))
//      .agg(avg(employeeDF("salary")), avg(employeeDF("age")))
//      .show()

    employeeDF.printSchema()

//    employeeDF.filter{
//      "age>20"
//    }
//      .join(departmentDF, $"depId" === $"id")
//      .groupBy(departmentDF("name"), employeeDF("gender"))
//      .agg(avg(employeeDF("salary")), avg(employeeDF("age"))).show()

    /**
      * 基础的知识带一下
      *
      * dataframe == dataset[Row]
      * dataframe的类型是Row，所以是untyped类型，弱类型
      * dataset的类型通常是我们自定义的case class，所以是typed类型，强类型
      *
      * dataset开发，与rdd开发有很多的共同点
      * 比如说，dataset api也分成transformation和action，transformation是lazy特性的
      * action会触发实际的计算和操作
      *
      * dataset也是有持久化的概念的
      */
  }
}
