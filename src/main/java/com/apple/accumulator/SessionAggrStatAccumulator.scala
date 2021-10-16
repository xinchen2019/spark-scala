package com.apple.accumulator

import org.apache.spark.sql.SparkSession
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

/**
  * @Program: spark-scala
  * @ClassName: SessionAggrStatAccumulator
  * @Description: 累加器
  * @Author Mr.Apple
  * @Create: 2021-10-13 10:57
  * @Version 1.1.0
  **/
object SessionAggrStatAccumulatorApp {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]")
      .appName("SessionAggrStatAccumulatorApp").getOrCreate()
    val sessionAggrStatAccumulator = new SessionAggrStatAccumulator;
    val sc = sparkSession.sparkContext
    sc.register(sessionAggrStatAccumulator, "acc")

    val rdd = sc.parallelize(List("a", "b", "c", "a", "c", "d", "a"))

    rdd.foreach { line =>
      sessionAggrStatAccumulator.add(line)
    }
    var i = 0


    //    for (i <- 0 to 10) {
    //    sessionAggrStatAccumulator.add("session_1")
    //    sessionAggrStatAccumulator.add("session_1")
    //      sessionAggrStatAccumulator.add("session_2");
    //    }
    val count_1 = sessionAggrStatAccumulator.value.get("a")
    val count_2 = sessionAggrStatAccumulator.value.get("b")
    println("a: " + count_1)
    println("b: " + count_2)
  }
}

/**
  * isZero: 当AccumulatorV2中存在类似数据不存在这种问题时，是否结束程序。
  * copy: 拷贝一个新的AccumulatorV2
  * reset: 重置AccumulatorV2中的数据
  * add: 操作数据累加方法实现
  * merge: 合并数据
  * value: AccumulatorV2对外访问的数据结果
  */
class SessionAggrStatAccumulator extends AccumulatorV2[String, mutable.HashMap[String, Int]] {
  // 保存所有聚合数据
  private val aggrStatMap = mutable.HashMap[String, Int]()

  override def isZero: Boolean = {
    println("===isZero===")
    aggrStatMap.isEmpty
  }

  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Int]] = {
    println("===copy===")
    val newAcc = new SessionAggrStatAccumulator
    aggrStatMap.synchronized {
      newAcc.aggrStatMap ++= this.aggrStatMap
    }
    newAcc
  }

  override def reset(): Unit = {
    println("===reset===")
    aggrStatMap.clear()
  }

  override def add(v: String): Unit = {
    println("===add===")
    if (!aggrStatMap.contains(v))
      aggrStatMap += (v -> 0)
    aggrStatMap.update(v, aggrStatMap(v) + 1)
  }

  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Int]]): Unit = {
    println("===merge===")
    other match {
      case acc: SessionAggrStatAccumulator => {
        (this.aggrStatMap /: acc.value) {
          case (map, (k, v)) =>
            map += (k -> (v + map.getOrElse(k, 0)))
        }
      }
    }
  }

  override def value: mutable.HashMap[String, Int] = {
    println("===value===")
    this.aggrStatMap
  }
}
