package com.apple.accumulator

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Program: spark-scala
  * @ClassName: MyAccumulatorDemo
  * @Description: TODO
  * @Author Mr.Apple
  * @Create: 2021-10-11 18:21
  * @Version 1.1.0
  **/
object MyAccumulatorDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("MyAccumulator")
    val sc = new SparkContext(conf)
    val accumulator = sc.longAccumulator("count")
    val rdd1 = sc.parallelize(10 to 100).map(x => {
      if (x % 2 == 0) {
        accumulator.add(1)
      }
    })
    println("count =   " + rdd1.count())
    println("accumulator =   " + accumulator.value)
    sc.stop()
  }
}
