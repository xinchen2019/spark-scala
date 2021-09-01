package com.apple.sql

import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Program: spark-scala
  * @ClassName: DailySale
  * @Description: TODO
  * @Author Mr.Apple
  * @Create: 2021-08-13 14:59
  * @Version 1.1.0
  **/
object DailySale {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("DailySale")
    val sc = new SparkContext(conf);
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._
    val userSaleLog = Array(
      "2015-10-01,55.05,1122",
      "2015-10-01,23.15,1133",
      "2015-10-01,15.20,",
      "2015-10-02,56.05,1144",
      "2015-10-02,78.87,1155",
      "2015-10-02,113.02,1123"
    )
    val userSaleLogRDD = sc.parallelize(userSaleLog, 5);
    val filteredUserSaleLogRDD = userSaleLogRDD.filter {
      log =>
        if (log.split(",").length == 3)
          true else false
    }
    val userSaleLogRowRDD =
      filteredUserSaleLogRDD.map { log => Row(log.split(",")(0), log.split(",")(1).toDouble)
      }

    val structType = StructType(Array(StructField("date", StringType, true),
      StructField("sale_amount", DoubleType, true)
    ))

    val userSaleLogDF = sqlContext.createDataFrame(userSaleLogRowRDD, structType);

    userSaleLogDF.groupBy("date")
      .agg('date, sum('sale_amount))
      .rdd
      .map { row => Row(row(1), row(2)) }
      .collect()
      .foreach(println)
  }
}
