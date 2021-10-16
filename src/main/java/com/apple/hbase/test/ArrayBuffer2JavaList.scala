package com.apple.hbase.test

import scala.collection.mutable.ArrayBuffer

/**
  * @Program: Default (Template) Project
  * @ClassName: ArrayBuffer2JavaList
  * @Description: TODO
  * @Author Mr.Apple
  * @Create: 2021-09-28 18:08
  * @Version 1.1.0
  **/
object ArrayBuffer2JavaList {
  def main(args: Array[String]): Unit = {
    // Scala集合和Java集合互相转换

    val arr = ArrayBuffer("1", "2", "3")
    /*
    implicit def bufferAsJavaList[A](b : scala.collection.mutable.Buffer[A]) : java.util.List[A] = { /* compiled code */ }
     */
    import scala.collection.JavaConversions.bufferAsJavaList
    //对象 ProcessBuilder ， 因为 这里使用到上面的  bufferAsJavaList
    val javaArr = new ProcessBuilder(arr) //为什么可以这样使用?
    // 这里arrList 就是java中的List
    val arrList = javaArr.command()

    println(arrList) //输出 [1, 2, 3]


    //java的List 转成 scala的 ArrayBuffer
    //说明
    //1. asScalaBuffer 是一个隐式函数
    /*
    implicit def asScalaBuffer[A](l : java.util.List[A]) : scala.collection.mutable.Buffer[A] = { /* compiled code */ }
     */
    import scala.collection.JavaConversions.asScalaBuffer
    import scala.collection.mutable
    // java.util.List ==> Buffer
    val scalaArr: mutable.Buffer[String] = arrList
    scalaArr.append("jack")
    scalaArr.append("tom")
    scalaArr.remove(0)
    println(scalaArr) // (2,3,jack,tom)

  }
}
