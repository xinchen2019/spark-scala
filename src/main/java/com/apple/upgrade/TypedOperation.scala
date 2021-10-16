package com.apple.upgrade

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * @Program: spark-scala
  * @ClassName: TypedOperation
  * @Description: type操作
  * @Author Mr.Apple
  * @Create: 2021-09-04 09:18
  * @Version 1.1.0
  **/
object TypedOperation {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder() //用到了java里面的构造设计模式
      .appName("TypedOperation")
      .master("local[*]")
      //这是Spark SQL 2.0 里面一个重要的变化，需要设置spark sql的元数据仓库的目录
      //.config("spark.sql.warehouse.dir", warehouseLocation)
      //启用hive支持
      .getOrCreate()
    import spark.implicits._

    val employeeDF = spark.read.json("data\\employee.json")
    val employeeDF2 = spark.read.json("data\\employee2.json")
    val departmentDF = spark.read.json("data\\department.json")

    val employeeDS = employeeDF.as[Employee]
    val employeeDS2 = employeeDF2.as[Employee]
    val departmentDS = departmentDF.as[Department]
    //println(employeeDS.rdd.partitions.size)

    /**
      * coalesce和repartition操作
      * 都是用来重新定义分区的
      * 区别在于：coalesce，只能用于减少分区数量，而且可以选择不发生shuffle
      * repartiton，可以增加分区，也可以减少分区，必须会发生shuffle，相当于是进行了一次重分区操作
      */
    val employeeDSRepartitioned = employeeDF.repartition(7)

    //println(employeeDSRepartitioned.rdd.partitions.size)

    val employeeDSCoalesced = employeeDSRepartitioned.coalesce(3);

    //println(employeeDSCoalesced.rdd.partitions.size)

    //employeeDSCoalesced.show()

    /**
      * except:获取在当前dataset中有，但是在另一个dataset中没有的元素
      * filter：根据我们自己的逻辑，如果反悔true，那么保留该元素，否则就过滤掉该元素
      * intersect：获取两个数据集的交集
      */

    //employeeDS.except(employeeDS2).show()
    //employeeDS.filter { employee => employee.age > 32 }.show
    //println("===========================================")
    //employeeDF.filter("age>30").show()
    //employeeDS.intersect(employeeDS2).show()
    //println("===========================================")
    //employeeDF.intersect(employeeDF2).show()

    //employeeDS.map(employee => (employee.name, employee.salary + 1000)).show()
    /**
      * map：将数据集中的每条数据都做一个映射，返回一条新数据
      * flatMap：数据集中的每条数据都可以返回多条数据
      * mapPartitions：一次性对一个partition中数据进行处理
      */
    //    departmentDS.flatMap {
    //      department =>
    //        Seq(Department(department.id + 1, department.name + "_1"),
    //          Department(department.id + 2, department.name + "_2"))
    //    }.show()
    //   println("=========================================")
    //    departmentDF.flatMap {
    //      department =>
    //        Seq(Department(department.getAs("id"), department.getAs("name")))
    //    }.show()
    //    employeeDS.mapPartitions { employees =>
    //      val result = scala.collection.mutable.ArrayBuffer[(String, Long)]()
    //      while (employees.hasNext) {
    //        var emp = employees.next()
    //        result += ((emp.name, emp.salary + 1000))
    //      }
    //      result.iterator
    //    }.show()
    //    println("=========================================")
    //    employeeDF.mapPartitions { employees =>
    //      val result = scala.collection.mutable.ArrayBuffer[(String, Long)]();
    //      while (employees.hasNext) {
    //        var emp = employees.next()
    //        result += ((emp.getAs[String]("name"), emp.getAs[Long]("salary") + 1000))
    //      }
    //      result.iterator
    //    }.show()

    //employeeDS.joinWith(departmentDS,$"depId"===$"id").show()

    //employeeDS.sort($"salary".desc).show()

    val employeeDSArr = employeeDS.randomSplit(Array(3, 10, 20))
    //employeeDSArr.foreach(_.show())
    employeeDS.sample(false, 0.3).show()
  }

  case class Employee(name: String, age: Long, depID: Long, gender: String, salary: Long)

  Logger.getLogger("org").setLevel(Level.ERROR)

  case class Department(id: Long, name: String)

}
