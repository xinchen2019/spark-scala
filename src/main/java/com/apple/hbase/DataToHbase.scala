package com.apple.hbase

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Program: spark-scala
  * @ClassName: DataToHbase
  * @Description: TODO
  * @Author Mr.Apple
  * @Create: 2021-08-14 13:28
  * @Version 1.1.0
  **/
object DataToHbase {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("HBaseTest1").setMaster("local")
    val sc = new SparkContext(sparkConf)

    val conf = HBaseConfiguration.create()
    //设置zooKeeper集群地址，也可以通过将hbase-site.xml导入classpath，但是建议在程序里这样设置
    conf.set("hbase.zookeeper.quorum", "master,slave1,slave2")
    //设置zookeeper连接端口，默认2181
    conf.set("hbase.zookeeper.property.clientPort", "2181")

    val tablename = "apple:account1"

    //初始化jobconf，TableOutputFormat必须是org.apache.hadoop.hbase.mapred包下的
    val jobConf = new JobConf(conf)
    jobConf.setOutputFormat(classOf[TableOutputFormat])

    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tablename)
    val indataRDD = sc.makeRDD(Array("4,jack,15", "5,Lily,16", "6,mike,16"))
    val rdd = indataRDD.map(_.split(',')).map { arr => {

      // 一个Put对象就是一行记录，在构造方法中指定主键
      // 所有插入的数据必须用org.apache.hadoop.hbase.util.Bytes.toBytes方法转换
      // Put.add方法接收三个参数：列族，列名，数据
      val put = new Put(Bytes.toBytes(arr(0)))
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(arr(1)))
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes(arr(2)))
      //转化成RDD[(ImmutableBytesWritable,Put)]类型才能调用saveAsHadoopDataset
      (new ImmutableBytesWritable, put)
    }
    }
    rdd.saveAsHadoopDataset(jobConf)

    sc.stop()
  }

}
