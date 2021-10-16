package com.apple.sql

import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

/**
  * @Program: spark-scala
  * @ClassName: AverageUserDefinedAggregateFunction
  * @Description: TODO
  * @Author Mr.Apple
  * @Create: 2021-10-16 17:06
  * @Version 1.1.0
  *          参考链接     https://www.cnblogs.com/cc11001100/p/9471859.html
  **/

object SparkSqlUDAFDemo_001 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("SparkSqlUDAFDemo_001")
      .getOrCreate()

    val sc = spark.sparkContext

    spark.udf.register("u_avg", AverageUserDefinedAggregateFunction)

    spark.read.json("data\\user\\user.json").createOrReplaceTempView("v_user")

    //将整张表看做是一个分组对所有人的平均年龄
    spark.sql("select count(1) as count,u_avg(age) as age_avg from v_user").show()
    //按照性别分组平均年龄
    spark.sql("select sex,count(1) as count,u_avg(age) as age_avg from v_user group by sex ").show()
  }
}

object AverageUserDefinedAggregateFunction extends UserDefinedAggregateFunction {
  //聚合函数的输入数据结构
  override def inputSchema: StructType = {
    println("===inputSchema===")
    StructType(StructField("input", LongType) :: Nil)
  }

  //缓存区数据结构
  override def bufferSchema: StructType = {
    println("===bufferSchema===")
    StructType(StructField("sum", LongType) :: StructField("count", LongType) :: Nil)
  }

  //聚合函数返回值数据结构
  override def dataType: DataType = {
    println("===dataType===")
    DoubleType
  }

  //聚合函数是否是幂等的，即相同输入是否总是得到相同输出
  override def deterministic: Boolean = {
    println("===deterministic===")
    true
  }

  //初始化缓冲区
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    println("===initialize===")
    buffer(0) = 0L
    buffer(1) = 0L
  }

  //给聚合函数传入一条新数据进行处理
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    println("===update===")
    if (input.isNullAt(0)) {
      return
    }
    buffer(0) = buffer.getLong(0) + input.getLong(0)
    buffer(1) = buffer.getLong(1) + 1
  }

  //合并聚合函数缓冲区
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    println("===merge===")
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  //计算最终结果
  override def evaluate(buffer: Row): Any = {
    println("===evaluate===")
    buffer.getLong(0).toDouble / buffer.getLong(1)
  }
}
