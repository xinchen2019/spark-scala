package com.apple.sql

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, StructType}

/**
  * @Program: spark-scala
  * @ClassName: StringCountUDTF
  * @Description: TODO
  * @Author Mr.Apple
  * @Create: 2021-08-13 20:40
  * @Version 1.1.0
  **/
object StringCount extends UserDefinedAggregateFunction {
  override def inputSchema: StructType = ???

  override def bufferSchema: StructType = ???

  override def dataType: DataType = ???

  override def deterministic: Boolean = ???

  override def initialize(buffer: MutableAggregationBuffer): Unit = ???

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = ???

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = ???

  override def evaluate(buffer: Row): Any = ???
}
