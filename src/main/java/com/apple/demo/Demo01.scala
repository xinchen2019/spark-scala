package com.apple.demo

/**
  * @Program: spark-scala
  * @ClassName: Demo01
  * @Description: TODO
  * @Author Mr.Apple
  * @Create: 2021-08-26 20:52
  * @Version 1.1.0
  **/
object Demo01 {

  val getStudentGrade: PartialFunction[String, Int] = {
    case "Leo" => 90;
    case "Jack" => 85;
    case "Marry" => 95
  }

  def main(args: Array[String]): Unit = {
    println(getStudentGrade.isDefinedAt("Leo"))
  }
}
