package test

import com.atguigu.streaming.{BaseKafkaProducerFactory, KafkaProducerProxy, PooledKafkaProducerAppFactory}
import org.apache.commons.pool2.impl.{GenericObjectPool, GenericObjectPoolConfig}

object KafkaStreaming {
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
