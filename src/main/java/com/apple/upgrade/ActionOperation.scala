package com.apple.upgrade

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * @Program: spark-scala
  * @ClassName: ActionOperation
  * @Description: action操作详解 collect、count、first、foreach、reduce、show、take
  * @Author Mr.Apple
  * @Create: 2021-09-03 23:03
  * @Version 1.1.0
  **/
object ActionOperation {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {

    //val warehouseLocation = "file:///${system:user.dir}/spark-warehouse"

    val spark = SparkSession
      .builder() //用到了java里面的构造设计模式
      .appName("ActionOperation")
      .master("local[*]")
      //这是Spark SQL 2.0 里面一个重要的变化，需要设置spark sql的元数据仓库的目录
      //.config("spark.sql.warehouse.dir", warehouseLocation)
      //启用hive支持
      .getOrCreate()

    import spark.implicits._
    val employeeDF = spark.read.json("data/employee.json")
    //collect:将分布式存储在集群上的分布式数据集（比如dataset），中的所有数据都获取到driver端来
    //employeeDF.collect().foreach(println(_))
    //count:对dataset中的记录数进行统计个数的操作
    // println(employeeDF.count())
    //first:获取数据集中的第一条数据
    //println(employeeDF.first())

    /**
      * foreach：遍历数据集中的每一条数据，对数据进行操作，这个跟collect不同，collect是将
      * 数据获取到driver端进行操作 foreach是将计算操作推到集群上去分布式进行
      * foreach(println(_)) 这种，真正在集群中执行的时候，是没用的，因为输出的结果是在分布式的集群中的，我们是看不到的
      */
    // employeeDF.foreach(println(_))

    //reduce：对数据集中的所有数据进行规约的操作，多条变成一条
    //用reduce来实现数据集的个数的统计
    println(employeeDF.map(employeeDF => 1).reduce(_ + _))
    //show，默认将dataset数据打印前20条
    // employeeDF.show()
    //take，从数据集中获取指定条数
    //employeeDF.take(3).foreach(println(_))
  }
}
