package com.atguigu.streaming

import java.util
import java.util.{Date, Random}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}

/**
  * HBase操作工具类
  *
  * @author huangfeifei
  */
object HBaseUtils {

  @transient private var hbaseConf: Configuration = null
  @transient private var connection: Connection = null





  def createConf(): Configuration = {
    if (hbaseConf == null) {
      hbaseConf = HBaseConfiguration.create()
      hbaseConf.set("hbase.zookeeper.quorum","192.168.119.103")
      hbaseConf.set("hbase.zookeeper.property.clientPort","2181")
    }
    hbaseConf
  }

  def createConnection(hbaseConf: Configuration): Connection = {
    if (connection == null) {
      connection = ConnectionFactory.createConnection(hbaseConf)
    }
    connection
  }

  def getTable(connection: Connection, tableName: TableName): Table = {
    connection.getTable(tableName)
  }
  def getAdmin(): Admin = {
//      new HBaseAdmin(HBaseUtils.createConf())
    val connection = ConnectionFactory.createConnection(HBaseUtils.createConf());       //工厂设计模式
   connection.getAdmin();
  }

  def deleteTable(tableName: TableName): Unit = {
    val admin=HBaseUtils.getAdmin()
    if (admin.tableExists(tableName)) {
      admin.disableTable(tableName)
      admin.deleteTable(tableName)
    }
  }

  def createTable(tableName: TableName, colFamilies: String*): Unit = {
    val admin=HBaseUtils.getAdmin()
    val tableDescriptor = new HTableDescriptor(tableName)
    colFamilies.foreach(colFamily => tableDescriptor.addFamily(new HColumnDescriptor(colFamily.getBytes)))
    if (!admin.tableExists(tableName)) {
      admin.createTable(tableDescriptor)
      println("创建成功")
    }else{
      println("表已经存在")
    }
  }

  /**
    * 生成当日的HBase表名
    *
    * @return
    */
  def formatTableName(htablePrefix: String): String = {
    val currentDay = DateUtils.dateBeforeFormat(0, DateUtils.DAY_FORMAT_PATTERN)
    new StringBuilder().append(htablePrefix).append("-").append(currentDay).toString
  }

  /**
    * 根据HBase节点数量和指定时间，生成随机分布的rowkey
    * TODO 有时间的话可以使用queue来实现，可以确保不重复
    *
    * @param nodes HBase节点数量
    * @return
    */
  def generateRowKey(nodes: Int, d: Date): String = {
    var sb = new StringBuilder
    val random = new Random
    val s = DateUtils.dateFormat(DateUtils.SECOND_FORMAT_PATTERN, d)
    sb.append(0).append(d.getTime % nodes).append(s)
    val r = random.nextInt(1000)
    if (r < 10) {
      sb = sb.append("00").append(r)
    } else if (r < 100) {
      sb = sb.append("0").append(r)
    } else {
      sb = sb.append(r)
    }
    sb.toString
  }

  def generateRowKey(numRegionServers: Int, rowKey: String): String = {
    val sb = new StringBuilder
    sb.append(Math.abs(rowKey.hashCode % numRegionServers)).append(rowKey).toString()
  }


  /**
    * 保存数据到HBase
    *
    * @param tableName
    * @param puts
    */
  @throws[Exception]
  def putData(tableName: String, puts: util.List[Put]): Unit = {
    if (connection == null || connection.isClosed) {
      connection = createConnection(createConf())
    }
    val tName: TableName = TableName.valueOf(tableName)
    val table: Table = connection.getTable(tName)
    table.put(puts)
  }

  @throws[Exception]
  def putData(tableName: String, put: Put): Unit = {
    if (connection == null || connection.isClosed) {
      connection = createConnection(createConf())
    }
    val tName: TableName = TableName.valueOf(tableName)
    val table: Table = connection.getTable(tName)
    table.put(put)
  }


  /**
    * 关闭连接
    *
    * @throws Exception
    */
  @throws[Exception]
  def closeConnection(): Unit = {
    if (connection != null && !connection.isClosed) {
      connection.close()
    }
  }
}
