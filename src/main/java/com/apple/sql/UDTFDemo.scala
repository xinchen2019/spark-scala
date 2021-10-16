package com.apple.sql

import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, ObjectInspectorFactory, StructObjectInspector}
import org.apache.spark.sql.SparkSession


/**
  * @Program: spark-scala
  * @ClassName: UserDefinedUDTF
  * @Description: TODO
  * @Author Mr.Apple
  * @Create: 2021-08-13 20:49
  * @Version 1.1.0
  *          参考链接 https://www.136.la/jingpin/show-130978.html#UDTF_137
  *          参考链接 https://blog.csdn.net/qq_41458071/article/details/106504945
  **/

object UDTFDemo {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("UDTFDemo")
      .enableHiveSupport()
      .getOrCreate()

    /**
      * val spark = SparkSession.builder().config("hive.metastore.uris","thrift://192.168.XXX.100:9083").master("local[*]").appName("utd")
      */
    val rdd = spark.sparkContext.textFile("data\\udtf\\udtf.txt")

    import spark.implicits._
    val infoDF = rdd.map(x => x.split("//")).filter(x => x(1).equals("ls"))
      .map(x => (x(0), x(1), x(2))).toDF("id", "name", "class")
    infoDF.createOrReplaceTempView("v_udtf")
    spark.sql("create temporary function myudtf as 'com.apple.sql.myUDTF'") //创建临时方法
    /**
      * +---+----+------+
      * | id|name|  type|
      * +---+----+------+
      * | 02|  ls|Hadoop|
      * | 02|  ls| scala|
      * | 02|  ls| kafka|
      * | 02|  ls|  hive|
      * | 02|  ls| hbase|
      * | 02|  ls| Oozie|
      * +---+----+------+
      */
    //spark.sql("select id,name,myudtf(class) from v_udtf").show() //使用自定义方法

    spark.sql("select id,name,clazz from v_udtf LATERAL VIEW myudtf(class) adTable AS clazz").show()
    //LATERAL VIEW ads_explode(adids) adTable AS adid
  }
}


class myUDTF extends GenericUDTF {

  //初始化
  override def initialize(argOIs: Array[ObjectInspector]): StructObjectInspector = {
    val fieldName = new java.util.ArrayList[String]()
    val fieldOIS = new java.util.ArrayList[ObjectInspector]()
    //定义输出数据类型
    fieldName.add("type")
    fieldOIS.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector) //String类型
    ObjectInspectorFactory.getStandardStructObjectInspector(fieldName, fieldOIS)
  }

  /**
    * 处理
    * 传入 Hadoop scala spark hive hbase
    * 输出 head  type String
    * Hadoop
    * scala
    * spark
    * hive
    * hbase
    */

  override def process(objects: Array[AnyRef]): Unit = {
    val strs = objects(0).toString.split(" ")
    for (str <- strs) {
      val temp = new Array[String](1)
      temp(0) = str
      forward(temp)
    }

  }

  override def close(): Unit = {

  }
}
