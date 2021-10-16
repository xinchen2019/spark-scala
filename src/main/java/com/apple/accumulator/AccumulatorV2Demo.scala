package com.apple.accumulator

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Program: spark-scala
  * @ClassName: AccumulatorV2Demo
  * @Description: TODO
  * @Author Mr.Apple
  * @Create: 2021-10-11 17:12
  * @Version 1.1.0
  *          参考链接http://www.hechunbo.com/index.php/archives/334.html
  **/
object AccumulatorV2Demo {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("AccumulatorV2Demo").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List(("tom", 11), ("Lilei", 12), ("Jerry", 13)))


    var myAcc = new MyAccumulator

    sc.register(myAcc)

    rdd.foreach {
      case (name, age) => {
        myAcc.add(age)
      }
    }
    println("----" + myAcc.value)
    sc.stop()
  }
}

class MyAccumulator extends AccumulatorV2[Int, Double] {
  var aggSum = 0
  var countSum = 0

  /**
    * 定义初始状态
    *
    * @return
    */
  override def isZero: Boolean = {
    println("====isZero===")
    aggSum == 0 && countSum == 0
  }

  /**
    * 拷贝，创建一个对象
    *
    * @return
    */
  override def copy(): AccumulatorV2[Int, Double] = {
    println("====copy===")
    var myAcc = new MyAccumulator
    myAcc.aggSum = this.aggSum
    myAcc.countSum = this.countSum
    myAcc
  }

  /**
    * 重置，执行的时候 先拷贝副本 再让副本归0
    */
  override def reset(): Unit = {
    println("====reset===")
    this.aggSum = 0
    countSum = 0
  }

  /**
    * 做累加，传一个值过去 值进行累加 人数进行加1操作
    *
    * @param age
    */
  override def add(age: Int): Unit = {
    println("====add===")
    aggSum += age
    countSum += 1
  }

  /**
    * 合并操作，如果传过来的对象MyAccumulator，则进行累加
    *
    * @param other
    */
  override def merge(other: AccumulatorV2[Int, Double]): Unit = {
    println("====merge===")
    other match {
      case ac: MyAccumulator => {
        this.aggSum += ac.aggSum
        this.countSum += ac.countSum
      }
    }
  }

  /**
    * 返回的结果
    *
    * @return
    */
  override def value: Double = {
    aggSum / countSum
  }
}
