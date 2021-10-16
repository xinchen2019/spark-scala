package com.apple.demo

import scala.collection.JavaConverters

/**
  * @Program: spark-scala
  * @ClassName: ScalaConverterJava
  * @Description: scala与java集合互转
  * @Author Mr.Apple
  * @Create: 2021-10-16 19:45
  * @Version 1.1.0
  **/
/**
  * implicit def asJavaIterator[A](it : scala.collection.Iterator[A]) : java.util.Iterator[A] = { /* compiled code */ }
  * implicit def asJavaEnumeration[A](it : scala.collection.Iterator[A]) : java.util.Enumeration[A] = { /* compiled code */ }
  * implicit def asJavaIterable[A](i : scala.collection.Iterable[A]) : java.lang.Iterable[A] = { /* compiled code */ }
  * implicit def asJavaCollection[A](it : scala.collection.Iterable[A]) : java.util.Collection[A] = { /* compiled code */ }
  * implicit def bufferAsJavaList[A](b : scala.collection.mutable.Buffer[A]) : java.util.List[A] = { /* compiled code */ }
  * implicit def mutableSeqAsJavaList[A](seq : scala.collection.mutable.Seq[A]) : java.util.List[A] = { /* compiled code */ }
  * implicit def seqAsJavaList[A](seq : scala.collection.Seq[A]) : java.util.List[A] = { /* compiled code */ }
  * implicit def mutableSetAsJavaSet[A](s : scala.collection.mutable.Set[A]) : java.util.Set[A] = { /* compiled code */ }
  * implicit def setAsJavaSet[A](s : scala.collection.Set[A]) : java.util.Set[A] = { /* compiled code */ }
  * implicit def mutableMapAsJavaMap[A, B](m : scala.collection.mutable.Map[A, B]) : java.util.Map[A, B] = { /* compiled code */ }
  * implicit def asJavaDictionary[A, B](m : scala.collection.mutable.Map[A, B]) : java.util.Dictionary[A, B] = { /* compiled code */ }
  * implicit def mapAsJavaMap[A, B](m : scala.collection.Map[A, B]) : java.util.Map[A, B] = { /* compiled code */ }
  * implicit def mapAsJavaConcurrentMap[A, B](m : scala.collection.concurrent.Map[A, B]) : java.util.concurrent.ConcurrentMap[A, B] = { /* compiled code */ }
  */
object ScalaConverterJava {
  def main(args: Array[String]): Unit = {

    /**
      * java ArrayList转成scala Seq
      */
    val fieldNames = new java.util.ArrayList[String]
    fieldNames.add("a")
    fieldNames.add("b")
    val fields = JavaConverters.asScalaIteratorConverter(fieldNames.iterator).asScala.toSeq
    //    for (field <- fields) {
    //      println(field)
    //    }

    /**
      * scala Seq 转成 java ArrayList
      */
    val tmpSeq = Seq("a", "b")
    val tmpList = scala.collection.JavaConversions.seqAsJavaList(tmpSeq)

    /**
      * scala Seq 转成 java buffer
      */
    import scala.collection.JavaConverters._
    val list = Seq(1, 2, 3, 4).asJava
    val buffer = list.asScala

    /**
      * scala List 转成 java ArrayList
      */
    import scala.collection.JavaConverters._
    val lists = List(1, 2, 3, 4).asJava

    val lis = new java.util.ArrayList[Int]()
    lis.add(1)
    lis.add(2)
  }
}
