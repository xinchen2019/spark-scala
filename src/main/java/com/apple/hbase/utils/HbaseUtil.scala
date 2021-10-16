package com.apple.hbase.utils

import java.text.DecimalFormat
import java.util

import com.apple.log.Logger
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer

/**
  * @Program: spark-scala
  * @ClassName: HbaseUtil
  * @Description: hbase工具类
  * @Author Mr.Apple
  * @Create: 2021-08-15 04:41
  * @Version 1.1.0
  **/
object HbaseUtil extends Logger {

  val conf = HBaseConfiguration.create()
  val conn = ConnectionFactory.createConnection(conf);
  private val connHolder = new ThreadLocal[Connection]

  conf.set("hbase.zookeeper.quorum", "master,slave1,slave2")
  conf.set("hbase.zookeeper.property.clientPort", "2181")
  private val adminHolder = new ThreadLocal[HBaseAdmin];

  //线程之间数据封闭，保证多线程情况下的数据安全
  adminHolder.set(conn.getAdmin().asInstanceOf[HBaseAdmin]);
  connHolder.set(conn);

  /**
    * 删除namespace
    *
    * @param dropNamespace
    */
  def dropNamespace(dropNamespace: String) = {
    val admin = adminHolder.get()
    admin.deleteNamespace(dropNamespace)
  }

  /**
    * 查看namespace是否存在
    *
    * @param namespace namespace
    * @return boolean
    */
  def namespaceExists(namespace: String): Boolean = {
    val admin = adminHolder.get()
    // 获取所有的命名空间
    val names = admin.listNamespaceDescriptors();
    for (desc <- names) {
      if (namespace.equals(desc.getName()))
        return true;
    }
    return false;
  }

  /**
    * 创建命名空間
    *
    * @param namespace namespace
    */
  def createNamespace(namespace: String) {
    val admin = adminHolder.get()
    try {
      admin.createNamespace(NamespaceDescriptor.create(namespace).build())
    } catch {
      case _: NamespaceExistException => println("The Namespace %s is Existing".format(namespace))

    }
  }

  /**
    * 创建表
    *
    * @param tableName
    * @param reginCount
    * @param columnFamily
    */
  def createTable(tableName: String, reginCount: Int, columnFamily: String*) {

    if (reginCount == 0) {
      val reginCount = 3
    }
    val admin = adminHolder.get()
    //判断表是否存在
    if (isTableExist(tableName)) {
      println("表" + tableName + "已存在")
    } else {
      //创建表属性对象,表名需要转字节
      val descriptor = new HTableDescriptor(TableName.valueOf(tableName))
      //创建多个列族
      for (cf <- columnFamily) {
        descriptor.addFamily(new HColumnDescriptor(cf))
      }
      //添加协处理器
      //descriptor.addCoprocessor("com.apple.phoenix.coprocessor.MyIndexCoprocess")
      //根据对表的配置，创建表
      admin.createTable(descriptor, genSplitKeys(reginCount))
      println("表" + tableName + "创建成功！")
    }
  }

  /**
    * 查看表是否存在
    *
    * @param tableName tableName
    * @return boolean
    */
  def isTableExist(tableName: String): Boolean = {
    val admin = adminHolder.get()
    return admin.tableExists(TableName.valueOf(tableName))
  }

  /**
    * hbase建表预分区
    *
    * @param regionCount
    * @return
    */
  def genSplitKeys(regionCount: Int): Array[Array[Byte]] = {
    val keys = new Array[String](regionCount)
    val df = new DecimalFormat("00")
    for (i <- 0 until regionCount) {
      keys(i) = df.format(i) + "|";
    }

    val splitKeys = new Array[Array[Byte]](regionCount)
    //生成byte[][]类型的分区键的时候，一定要保证分区键是有序的
    val treeSet = new util.TreeSet[Array[Byte]](Bytes.BYTES_COMPARATOR)
    for (i <- 0 until regionCount) {
      treeSet.add(Bytes.toBytes(keys(i)));
    }
    val splitKeysIterator = treeSet.iterator();
    var index = 0;
    while (splitKeysIterator.hasNext) {
      val b = splitKeysIterator.next();
      splitKeys(index) = b
      index += 1
    }
    return splitKeys
    //println(keys.mkString(" "))
  }

  /**
    * 删除表
    *
    * @param tableName tableName
    */
  def dropTable(tableName: String) {
    val admin = adminHolder.get()
    if (isTableExist(tableName)) {
      admin.disableTable(tableName)
      admin.deleteTable(tableName)
      println("表" + tableName + "删除成功！")
    } else {
      println("表" + tableName + "不存在！")
    }
  }

  /**
    * 删除表的多行数据
    *
    * @param tableName tableName
    * @param rows      rows
    *
    */
  def deleteMultiRow(tableName: String, rows: String*) {
    val table = connHolder.get().getTable(TableName.valueOf(tableName))
    val deleteList = new ListBuffer[Delete]()
    for (row <- rows) {
      val delete = new Delete(Bytes.toBytes(row))
      deleteList += delete
    }
    import scala.collection.JavaConverters._
    table.delete(deleteList.asJava)
    table.close();
  }

  /**
    * 查询某张表下的所有数据
    *
    * @param tableName tableName
    */
  def getAllRows(tableName: String) {
    val table = connHolder.get().getTable(TableName.valueOf(tableName))
    //得到用于扫描region的对象
    val scan = new Scan()
    //使用HTable得到resultcanner实现类的对象
    val resultScanner: ResultScanner = table.getScanner(scan)
    val results = resultScanner.iterator()
    while (results.hasNext()) {
      val cells = results.next().rawCells()
      for (cell: Cell <- cells) {
        //得到rowkey
        println("行键: " + Bytes.toString(CellUtil.cloneRow(cell)))
        //得到列族
        println("列族: " + Bytes.toString(CellUtil.cloneFamily(cell)))
        println("列: " + Bytes.toString(CellUtil.cloneQualifier(cell)))
        println("值: " + Bytes.toString(CellUtil.cloneValue(cell)))
      }
    }
  }

  /**
    * 获取某一行数据
    *
    * @param tableName tableName
    * @param rowKey    rowKey
    */
  def getRow(tableName: String, rowKey: String) = {
    val table = connHolder.get().getTable(TableName.valueOf(tableName))
    //HTable table = new HTable(conf, tableName);
    val get = new Get(Bytes.toBytes(rowKey))
    //get.setMaxVersions();显示所有版本
    //get.setTimeStamp();显示指定时间戳的版本
    val result = table.get(get)
    for (cell: Cell <- result.rawCells()) {
      println("行键:" + Bytes.toString(result.getRow()))
      println("列族" + Bytes.toString(CellUtil.cloneFamily(cell)))
      println("列:" + Bytes.toString(CellUtil.cloneQualifier(cell)))
      println("值:" + Bytes.toString(CellUtil.cloneValue(cell)))
      println("时间戳:" + cell.getTimestamp())
    }
  }

  /**
    * 获取"列族:列"的数据
    *
    * @param tableName tableName
    * @param rowKey    rowKey
    * @param family    family
    * @param qualifier qualifier
    */
  def getRowQualifier(tableName: String, rowKey: String, family: String, qualifier: String) = {
    val table = connHolder.get().getTable(TableName.valueOf(tableName))
    val get = new Get(Bytes.toBytes(rowKey))
    get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier))
    val result = table.get(get)
    for (cell <- result.rawCells()) {
      log("行键: " + Bytes.toString(result.getRow()))
      log("列族: " + Bytes.toString(CellUtil.cloneFamily(cell)))
      log("列: " + Bytes.toString(CellUtil.cloneQualifier(cell)))
      log("值: " + Bytes.toString(CellUtil.cloneValue(cell)))
    }
  }

  /**
    * 关闭Connection连接
    *
    */
  def close() = {
    val conn: Connection = connHolder.get();
    if (conn != null) {
      conn.close();
    }
  }

  /**
    * 插入一条数据
    *
    * @param tableName tableName
    * @param rowKey    rowKey
    * @param family    family
    * @param column    column
    * @param value     value
    */
  def insertData(tableName: String, rowKey: String, family: String, column: String, value: String) = {
    val conn = connHolder.get()
    val table = conn.getTable(TableName.valueOf(tableName))
    val put: Put = new Put(Bytes.toBytes(rowKey))
    put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value))
    table.put(put)
    table.close()
  }

  def batchInsertData(tableName: String, values: List[(String, String, String)]): Unit = {
    val maxSize: Int = 1024
    val conn = connHolder.get()
    import org.apache.hadoop.hbase.client.BufferedMutatorParams
    val params = new BufferedMutatorParams(TableName.valueOf(tableName)).writeBufferSize(maxSize)
    //开启hbase插入异常监控
    val listener = new BufferedMutator.ExceptionListener {
      override def onException(e: RetriesExhaustedWithDetailsException, mutator: BufferedMutator): Unit = {
        var i = 0
        while (i < e.getNumExceptions) {
          println("插入失败 " + e.getRow(i) + ".")
        }
      }
    }
    params.listener(listener)
    val mutator: BufferedMutator = conn.getBufferedMutator(params)
    var scalaList = values.map(t => {
      val rowkey = t._1
      val family = "info"
      val put: Put = new Put(Bytes.toBytes(rowkey))
      put.addColumn(Bytes.toBytes(family), "name".getBytes(), Bytes.toBytes(t._2))
      put.addColumn(Bytes.toBytes(family), "age".getBytes(), Bytes.toBytes(t._3))
      put
    })
    //批量写出
    import scala.collection.JavaConverters._
    mutator.mutate(scalaList.asJava)
    mutator.flush()
    mutator.close()
  }

  /**
    *
    * @param rowkey
    * @param regionCount
    * @return
    */
  def genRegionNum(rowkey: String, regionCount: Int): String = {
    var regionNum: Int = 0;
    var hash = rowkey.hashCode;
    if (regionCount > 0 && (regionCount & (regionCount - 1)) == 0) {
      //2n
      regionNum = hash & (regionCount - 1)
    } else {
      regionNum = hash % (regionCount - 1)
    }
    return regionNum + "_" + rowkey
  }

  def main(args: Array[String]): Unit = {
    //    val zhangsan = genRegionNum("lisi", 3)
    //    println(zhangsan)
    genSplitKeys(3);
  }
}
