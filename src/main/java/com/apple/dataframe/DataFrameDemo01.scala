package com.apple.dataframe

import org.apache.spark.sql.SparkSession

/**
  * @Program: spark-scala
  * @ClassName: DataFrameDemo01
  * @Description: dataFrame 常见的用法
  * @Author Mr.Apple
  * @Create: 2021-09-04 18:57
  * @Version 1.1.0
  **/
class DataFrameDemo01 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder() //用到了java里面的构造设计模式
      .appName("ActionOperation")
      .master("local[*]")
      //这是Spark SQL 2.0 里面一个重要的变化，需要设置spark sql的元数据仓库的目录
      //.config("spark.sql.warehouse.dir", warehouseLocation)
      //启用hive支持
      .getOrCreate()
    import spark.implicits._
    val df = spark.createDataset(
      Seq(
        ("a", 1),
        ("a", 2),
        ("b", 2),
        ("b", 3),
        ("c", 1)
      )).toDF("id", "num")
    //    val sc = spark.sparkContext
    ////    val df = sc.parallelize(
    ////      Seq(
    ////        ("a", 1),
    ////        ("a", 2),
    ////        ("b", 2),
    ////        ("b", 3),
    ////        ("c", 1)
    ////      )
    ////    ).toDF("id", "num")
    df.filter($"num" === 2)
    df.filter($"num" > 2)
    df.filter($"num" < 2)
    df.filter("num=2")
    df.filter("num>2")
    df.filter("num<2")
    val ind: Int = 2;
    df.filter($"num" === ind)
    df.filter($"num" > ind)
    df.filter($"num" < ind)

    //对字符串过滤
    df.filter($"id".equalTo("a"))
    //传递参数过滤
    val str = s"a"
    df.filter($"id" equalTo (str))

    /**
      * 多条件判断
      * 逻辑连接符 &&（并）、||（或）
      */

    df.filter($"num" === 2 && $"id".equalTo("a"))
    df.filter($"num" === 1 || $"num" === 3)

    df.filter(
      $"num" > 30)

  }
}
