package com.apple.atguigu.structured.streaming.day02

import org.apache.spark.sql.SparkSession

/**
  * @Program: spark-scala
  * @ClassName: SteamingStatic
  * @Description: TODO
  * @Author Mr.Apple
  * @Create: 2021-09-07 20:33
  * @Version 1.1.0
  **/
object SteamingStatic {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("SteamingStatic")
      .getOrCreate()
    import spark.implicits._
    val arr = Array(("lisi", 20), ("zs", 10), ("ww", 15))
    val staticDF = spark.createDataset(arr).toDF("name", "age")
    val steamingDF = spark.readStream
      .format("socket")
      .option("host", "master")
      .option("port", "9999")
      .load()
      .as[String]
      .map(line => {
        val splited = line.split(",")
        (splited(0), splited(1))
      }).toDF("name", "sex")
    val joinedDF = steamingDF.join(staticDF, Seq("name")) //"Left"
    joinedDF.writeStream
      .format("console")
      .outputMode("update")
      .start()
      .awaitTermination()
  }
}
