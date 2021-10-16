package com.apple.hbase.utils

import com.apple.hbase.utils.HbaseUtil.close

import scala.collection.mutable.ListBuffer

/**
  * @Program: spark-scala
  * @ClassName: hbaseApp
  * @Description: TODO
  * @Author Mr.Apple
  * @Create: 2021-08-15 16:29
  * @Version 1.1.0
  **/
object HbaseApp {
  def main(args: Array[String]): Unit = {
    val namespace = "apple"
    val tableName = "apple:account"
    val family = "info"
    //HbaseUtils.dropTable(tableName)
    //HbaseUtils.dropNamespace(namespace)
    //val arr = Array("4", "jack", "15")
    val userinfos = Array(
      "1001,Jack,11",
      "1002,Tom,12",
      "1003,Herry,13",
      "1004,Sam,14",
      "1005,Lily,15",
      "1006,Liping,16"
    )
    //HbaseUtil.createNamespace(namespace)
    //    val isExists = HbaseUtil.namespaceExists(namespace)
    //    println("isExists: " + isExists)
    //HbaseUtil.createTable(tableName, reginCount = 3, family)

    for (userinfo <- userinfos) {
      val rowkey = userinfo.split(",")(0)
      val name = userinfo.split(",")(1)
      val age = userinfo.split(",")(2)
      //HbaseUtils.insertData(tableName, rowkey, family, "name", name);
      //HbaseUtils.insertData(tableName, rowkey, family, "age", age);
    }
    //    HbaseUtils.getRowQualifier("apple:account", "4", "info", "name")
    //HbaseUtils.deleteMultiRow(tableName, "4") /**/
    //HbaseUtils.getAllRows(tableName)
    //values: List[(String,String, Int)])
    val userInfolist = new ListBuffer[(String, String, String)]
    //userInfolist:+(("1008", "Lucy","15"))
    //    userInfolist.append(("1008", "Lucy", "15"))
    //    userInfolist.append(("1009", "Andy", "19"))
    userInfolist.append(("1011", "刘华", "20"))
    HbaseUtil.batchInsertData(tableName, userInfolist.toList)

    getAllRows(tableName)
    close();
  }

  def getAllRows(tableName: String): Unit = {
    val rows = HbaseUtil.getAllRows(tableName)
    println(rows)
  }
}
