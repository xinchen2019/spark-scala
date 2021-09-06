package com.apple.upgrade

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * @Program: spark-scala
  * @ClassName: BasicOperation
  * @Description: TODO
  * @Author Mr.Apple
  * @Create: 2021-09-04 08:55
  * @Version 1.1.0
  **/
/**
  * 基础操作
  * 持久化：cache、persist
  * 创建临时视图：createTempView、createOrReplaceTempView
  * 获取执行计划：explain
  * 写数据到外部存储：write
  * dataset与dataframe互相转换：as、toDF
  */
object BasicOperation {

  def main(args: Array[String]): Unit = {

    //val warehouseLocation = "file:///${system:user.dir}/spark-warehouse"


    val spark = SparkSession
      .builder() //用到了java里面的构造设计模式
      .appName("BasicOperation")
      .master("local[*]")
      //这是Spark SQL 2.0 里面一个重要的变化，需要设置spark sql的元数据仓库的目录
      //.config("spark.sql.warehouse.dir", warehouseLocation)
      //启用hive支持
      .getOrCreate()
    import spark.implicits._
    val employeeDF = spark.read.json("data/employee.json")

    employeeDF.cache()
    //println(employeeDF.count())
    //println(employeeDF.count())

    employeeDF.createOrReplaceTempView("employee")
    //spark.sql("select * from employee where age>30").show()


    /**
      * 获取spark sql的执行计划
      * dataframe/dataset，比如执行了一个sql语句获取的dataframe，实际上内部包含一个logical plan，逻辑执行计划
      * 设计执行的时候，首先会通过底层的catalyst optimizer，生成物理执行计划，比如说会做一些优化，比如push filter
      * 还会通过whole-stage code generation技术去自动化生成代码，提升执行性能
      */
    //spark.sql("select * from employee where age > 30").explain()
    //employeeDF.printSchema()

    val employeeWithAgeGreaterThen30DF = spark.sql("select * from employee where age>30")
    //employeeWithAgeGreaterThen30DF.write.json("dataOut/employeeWithAgeGreaterThen30DF.json")

    val employeeDS = employeeDF.as[Employee]
    employeeDS.show()
    employeeDS.printSchema()

    val employeeDF2 = employeeDS.toDF()
  }

  Logger.getLogger("org").setLevel(Level.ERROR)

  case class Employee(name: String, age: Long, depID: Long, gender: String, salary: Long)
}
