package com.apple.accumulator

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Program: spark-scala
  * @ClassName: AccumulatorVariable
  * @Description: TODO
  * @Author Mr.Apple
  * @Create: 2021-10-11 18:12
  * @Version 1.1.0
  **/
object AccumulatorVariable {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("AccumulatorVariable")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val sum = sc.accumulator(0)
    val numberArray = Array(1, 2, 3, 4, 5)
    val numbers = sc.parallelize(numberArray, 1)
    numbers.foreach { num => sum += num }
    println(sum)
  }
}
