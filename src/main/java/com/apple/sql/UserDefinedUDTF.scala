package com.apple.sql

import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF


/**
  * @Program: spark-scala
  * @ClassName: UserDefinedUDTF
  * @Description: TODO
  * @Author Mr.Apple
  * @Create: 2021-08-13 20:49
  * @Version 1.1.0
  **/
class UserDefinedUDTF extends GenericUDTF {
  override def process(objects: Array[AnyRef]): Unit = ???

  override def close(): Unit = ???
}
