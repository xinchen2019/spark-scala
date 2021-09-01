package com.apple.demo

/**
  * @Program: spark-scala
  * @ClassName: Object
  * @Description: TODO
  * @Author Mr.Apple
  * @Create: 2021-08-26 22:07
  * @Version 1.1.0
  **/


class Person(name: String, age: Int)

object Person {
  def apply(name: String, age: Int) = new Person(name, age)

  def main(args: Array[String]): Unit = {
    val Person(name, age) = "leo 25"
    println(Person)
  }

  def unapply(str: String) = {
    val splitIndex = str.indexOf(" ")
    if (splitIndex == -1) None
    else Some((str.substring(0, splitIndex), str.substring(splitIndex + 1)))
  }
}

