package com.apple.phoenix.coprocessor

import org.apache.hadoop.hbase.client.{ConnectionFactory, Durability, Put}
import org.apache.hadoop.hbase.coprocessor.{BaseRegionObserver, ObserverContext, RegionCoprocessorEnvironment}
import org.apache.hadoop.hbase.regionserver.wal.WALEdit
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}


/**
  * @Program: spark-scala
  * @ClassName: MyIndexCoprocess
  * @Description: TODO
  * @Author Mr.Apple
  * @Create: 2021-08-21 15:16
  * @Version 1.1.0
  **/
class MyIndexCoprocess extends BaseRegionObserver {

  override def postPut(e: ObserverContext[RegionCoprocessorEnvironment], put: Put, edit: WALEdit, durability: Durability) = {

    //获取Connection对象
    val conn = ConnectionFactory.createConnection(HBaseConfiguration.create())

    //获取Index表对象
    val index = conn.getTable(TableName.valueOf(""))

    //执行数据插入
    index.put(put)

    //关闭资源
    index.close()
    conn.close()
  }
}
