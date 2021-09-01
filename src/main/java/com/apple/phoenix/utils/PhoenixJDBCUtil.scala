package com.apple.phoenix.utils

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

/**
  * @Program: spark-scala
  * @ClassName: PhoenixJDBCUtil
  * @Description: TODO
  * @Author Mr.Apple
  * @Create: 2021-08-21 09:55
  * @Version 1.1.0
  **/
object PhoenixJDBCUtil {

  val driver = "org.apache.phoenix.jdbc.PhoenixDriver"
  val url = "jdbc:phoenix:master,slave1,slave2:2181"

  var conn: Connection = null
  var pstmt: PreparedStatement = null
  var rs: ResultSet = null

  def main(args: Array[String]): Unit = {

    Class.forName(driver)
    conn = DriverManager.getConnection(url)

    //预编译
    pstmt = conn.prepareStatement("select * from STOCK_SYMBOL")

    //执行
    rs = pstmt.executeQuery()
    while (rs.next()) {
      println(rs.getString(1))
      println(rs.getString(2))
    }
    //关闭资源
    rs.close()
    pstmt.close()
    conn.close()
  }
}
