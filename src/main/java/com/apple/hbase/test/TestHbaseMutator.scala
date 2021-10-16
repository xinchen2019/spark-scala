package com.apple.hbase.test

import java.io.IOException
import java.util
import java.util.concurrent.{Executors, TimeUnit}

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}


/**
  * @Program: spark-scala
  * @ClassName: Test
  * @Description: TODO
  * @Author Mr.Apple
  * @Create: 2021-08-15 23:07
  * @Version 1.1.0
  **/
/**
  * 参考链接 https://blog.csdn.net/weixin_34138521/article/details/92378270
  */
object TestHbaseMutator {
  def main(args: Array[String]): Unit = {
    val config = HBaseConfiguration.create
    //    // 一定要添加下面两个配置，不然找不到自己的Hbase位置
    //    config.set("hbase.rootdir", "hdfs://crxy99:9000/hbase")
    //    // 使用eclipse时必须添加这个，否则无法定位
    //    config.set("hbase.zookeeper.quorum", "crxy99,crxy100,crxy101")
    config.set("hbase.zookeeper.quorum", "master,slave1,slave2")
    config.set("hbase.zookeeper.property.clientPort", "2181")
    //获取链接
    val connection = ConnectionFactory.createConnection(config)
    // 创建一个线程池，里面有10线程
    val threadPool = Executors.newFixedThreadPool(10)

    val scheduledThreadPool = Executors.newScheduledThreadPool(1)
    //开启线程池监控
    /**
      * ScheduledFuture<?> result = executor.scheduleAtFixedRate(task,2, 5, TimeUnit.SECONDS);
      *
      * 在延迟2秒之后开始执行首个任务，之后每隔5秒执行一个任务，也就是固定间隔时间执行一次任务，而不是等到上个任务执行结束。
      */
    scheduledThreadPool.scheduleAtFixedRate(new Runnable() {
      override def run(): Unit = {
        println(threadPool)
      }
    }, 3, 5, TimeUnit.SECONDS)
    //向hbase中插入一千万条数据
    for (i <- 8000 to 10000) {
      val basix = i
      threadPool.submit(new Runnable() {
        override def run(): Unit = {
          try {
            val mutator = new util.ArrayList[Mutation]
            for (j <- 0 to 10000) {
              val puts = new Put(Bytes.toBytes(basix))
              puts.addColumn(Bytes.toBytes("f6"), Bytes.toBytes(basix + 2), Bytes.toBytes(basix + j))
              mutator.add(puts)
              println("===mutator.add(puts)===")
            }
            mutatorToHbase(connection, "t6", mutator)
          } catch {
            case e: IOException =>
              e.printStackTrace()
          }
        }
      })
    }
  }

  //向hbase插入数据
  def mutatorToHbase(connect: Connection, tableName: String, list: util.List[Mutation]): Unit = {
    val params = new BufferedMutatorParams(TableName.valueOf(tableName))
    //开启hbase插入异常监控
    val listeners = new BufferedMutator.ExceptionListener() {
      override def onException(e: RetriesExhaustedWithDetailsException, mutator: BufferedMutator): Unit = {
        var i = 0
        while (i < e.getNumExceptions) {
          println("e: " + e)
          println("插入失败 " + e.getRow(i) + ".")
        }
      }
    }
    //开启异常监听
    params.listener(listeners)
    val bufferedMutator = connect.getBufferedMutator(params)
    //批量提交数据插入。
    bufferedMutator.mutate(list)
    println("===bufferedMutator.mutate===")
    bufferedMutator.close()
  }
}
