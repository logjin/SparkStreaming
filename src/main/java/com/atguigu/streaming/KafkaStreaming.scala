package com.atguigu.streaming

import java.util

import kafka.common.TopicAndPartition
import org.apache.commons.lang3.StringUtils
import org.apache.commons.pool2.impl.{GenericObjectPool, GenericObjectPoolConfig}
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.api.java.function.VoidFunction
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scala.collection.JavaConversions._
/**
  * Created by wuyufei on 06/09/2017.
  */

//单例对象
object createKafkaProducerPool{

  //用于返回真正的对象池GenericObjectPool
  def apply():  GenericObjectPool[KafkaProducerProxy] = {
    val producerFactory = new BaseKafkaProducerFactory("192.168.119.101:9092", defaultTopic = Option("lj"))
    val pooledProducerFactory = new PooledKafkaProducerAppFactory(producerFactory)
    //指定了你的kafka对象池的大小
    val poolConfig = {
      val c = new GenericObjectPoolConfig
      val maxNumProducers = 10
      c.setMaxTotal(maxNumProducers)
      c.setMaxIdle(maxNumProducers)
      c
    }
    //返回一个对象池
    new GenericObjectPool[KafkaProducerProxy](pooledProducerFactory, poolConfig)
  }
}

object KafkaStreaming{

  def main(args: Array[String]) {

    //设置sparkconf
    val conf = new SparkConf().setMaster("local[1]").setAppName("NetworkWordCount")
    //新建了streamingContext
    val ssc = new StreamingContext(conf, Seconds(1))

    //kafka的地址
    val brobrokers = "192:9092"
    //kafka的队列名称
    val sourcetopic="source";
    //kafka的队列名称
    val targettopic="target";

    //创建消费者组名
    var group="con-consumer-group"

    //kafka消费者配置
    val kafkaParam = Map(
      "bootstrap.servers" -> "192:9092",//用于初始化链接到集群的地址
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
//      //用于标识这个消费者属于哪个消费团体
      "group.id" -> "g1",
//      //如果没有初始化偏移量或者当前的偏移量不存在任何服务器上，可以使用这个配置属性
//      //可以使用这个配置，latest自动重置偏移量为最新的偏移量
      "auto.commit.interval.ms" -> "1000",
//      //如果是true，则这个消费者的偏移量会在后台自动提交
    "enable.auto.commit" -> (false: java.lang.Boolean) )
//      //ConsumerConfig.GROUP_ID_CONFIG
    /**
      * props.put("enable.auto.commit", "true");
		// 自动确认offset的时间间隔
		props.put("auto.commit.interval.ms", "1000");
      */

    //创建DStream，返回接收到的输入数据
    val columnNameArr = Array("lj","jimmy","szy")
    val primaryKeysArr = Array("lj","jimmy","szy")
    var stream: InputDStream[ConsumerRecord[String, String]] =KafkaUtils.createDirectStream[String,String](ssc, LocationStrategies.PreferConsistent,ConsumerStrategies.Subscribe[String,String](Array(sourcetopic),kafkaParam))

    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]

    stream.transform(rdd => {
      // 利用transform取得OffsetRanges
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      rdd.map(rdd =>(rdd.value()))

    }).foreachRDD(rdd => {

      //对于RDD的每一个分区执行一个操作
      rdd.foreachPartition(partitionOfRecords => {
        // kafka连接池。
        val pool = createKafkaProducerPool()
        //从连接池里面取出了一个Kafka的连接
        val p = pool.borrowObject()
        //发送当前分区里面每一个数据
        partitionOfRecords.foreach {message => System.out.println(message)

          val fieldValueList: Array[String] = message.toString().split(",")
if (fieldValueList.length>2) {

  val columnMap = new util.HashMap[String, String]

  for (i <- columnNameArr.indices) {
    columnMap.put(columnNameArr(i), fieldValueList(i))
  }
  // 生成rowKey
  val primaryKeyValue = new util.ArrayList[String]()
  for (i <- primaryKeysArr.indices) {
    primaryKeyValue.add(columnMap.get(primaryKeysArr(i)))
  }
  val rowKeySuffix = StringUtils.join(primaryKeyValue, "_")
  val rowKey = rowKeySuffix.hashCode % 5 + rowKeySuffix
  // 生成rowKey

  val put = new Put(Bytes.toBytes(rowKey))

  for (i <- columnNameArr.indices) {
    columnMap.put(columnNameArr(i), fieldValueList(i))
  }

  for ((k, v) <- columnMap) {
    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(k), Bytes.toBytes(v))
  }
  HBaseUtils.putData("gs_test02", put)

}


        }
        // 使用完了需要将kafka还回去
        pool.returnObject(p)
      })

      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    })


    ssc.start()
    ssc.awaitTermination()

  }

}
