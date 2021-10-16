package com.apple.demo

/**
  * @Program: spark-scala
  * @ClassName: FoldLeft
  * @Description: TODO
  * @Author Mr.Apple
  * @Create: 2021-10-11 18:41
  * @Version 1.1.0
  **/
object FoldLeft {
  def main(args: Array[String]): Unit = {
    val map1 = scala.collection.mutable.Map("G01" -> 1, "G02" -> 10)
    val map2 = scala.collection.mutable.Map("G02" -> 5, "G03" -> 2)
    val a = map1.foldLeft(map2)(
      (innerMap, kv) => {
        println("innerMap:" + innerMap)
        println("kv:" + kv)
        println("kv:" + kv._1)
        innerMap(kv._1) = innerMap.getOrElse(kv._1, 0) + kv._2
        innerMap
      }
    )
    println("a" + a)
  }
}
