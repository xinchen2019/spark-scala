package com.apple.demo

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Program: spark-scala
  * @ClassName: CountByKeyDemo
  * @Description: TODO
  * @Author Mr.Apple
  * @Create: 2021-10-12 14:02
  * @Version 1.1.0
  **/
object CountByKeyDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("countdemo");
    val sc = new SparkContext(conf);
    val arr = Array(("class1", "tele"), ("class1", "yeye"), ("class2", "wyc"));
    val rdd = sc.parallelize(arr, 1);
    val result = rdd.countByKey();
    for ((k, v) <- result) {
      println(k + ":" + v);
    }
  }
}