package com.apple.upgrade

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}

/**
  * @Program: spark-scala
  * @ClassName: SparkSQLDemo
  * @Description: TODO
  * @Author Mr.Apple
  * @Create: 2021-09-03 19:23
  * @Version 1.1.0
  **/


object SparkSQLDemo {

  val warehouseLocation = "file:///${system:user.dir}/spark-warehouse"

  def main(args: Array[String]): Unit = {
    //构建SparkSession，基于build()来构造
    val spark = SparkSession
      .builder() //用到了java里面的构造设计模式
      .appName("SparkSQLDemo")
      .master("local[*]")
      //这是Spark SQL 2.0 里面一个重要的变化，需要设置spark sql的元数据仓库的目录
      //.config("spark.sql.warehouse.dir", warehouseLocation)
      //启用hive支持
      .enableHiveSupport()
      .getOrCreate()


    import spark.implicits._

    //读取json文件，构造一个untyped弱类型的dataframe
    //dataframe就相当于Datset[Row]

    val df = spark.read.json("data/people.json")
    //df.show() //打印数据
    //df.printSchema() //打印元数据
    //df.select("name").show() //select操作，典型的弱类型 untype操作
    //df.select($"name", $"age" + 1).show() //使用表达式，scala的语法，要用$符号作为前缀
    //df.filter($"age" > 21).show() //filter操作表达式的一个应用
    //df.groupBy("age").count().show()

    /**
      * createOrReplaceTempView：创建临时视图，此视图的生命周期与用于创建此数据集的[SparkSession]相关联。
      * createGlobalTempView：创建全局临时视图，此时图的生命周期与Spark Application绑定。
      */
    df.createOrReplaceTempView("people")
    val sqlDF = spark.sql("select * from people")
    //sqlDF.show()

    //基于jvm object来构造dataset
    val caseClassDS = Seq(Person("Andy", 32)).toDS
    //caseClassDS.show()
    //基于原始数据类型构造dataset
    val primitiveDS = Seq(1, 2, 3).toDS()
    //primitiveDS.map(_ + 1).show()
    //基于已有结构化数据文件，构造dateset
    val path = "data/people.json"
    /**
      *spark.read.json()，首先获取到的是cataframe,
      * 其次使用as[Person]之后，就可以将一个dataframe转换成一个dataset
      */
    val peopleDS = spark.read.json(path).as[Person]
    //peopleDS.show()

    //spark.sql("show databases")
    //CREATE TABLE IF NOT EXISTS src (key INT, value STRING)
    //spark.sql("create table if not exists src (key int,value string)")
    //spark.sql("LOAD DATA " + "LOCAL INPATH 'data/kv1.txt' " + "INTO TABLE src")
    //spark.sql("select * from src").show()
    //spark.sql("select count(*) from src").show()

    val hiveDF = spark.sql("select key,value from src where key <10 order by key")
    //hiveDF.show()
    val hiveDS = hiveDF.map {
      case Row(key: Int, value: String) => s"Key: $key, Value: $value"
    }
    hiveDS.show()

    val recordsDF = spark.createDataFrame((1 to 100)
      .map(i => Record(i, s"val_$i")))
    recordsDF.createOrReplaceTempView("records")
    spark.sql("SELECT * FROM records r JOIN src s ON r.key = s.key")
      .show()
  }

  Logger.getLogger("org").setLevel(Level.ERROR)

  /**
    * 定义一个case class
    * 会用dataset，通常都会通过case class定义dataset的数据结构，
    * 自定义类型其实就是一种强类型，也是typed
    */
  case class Person(name: String, age: Long)

  //定义Hive case class
  case class Record(key: Int, value: String)
}
