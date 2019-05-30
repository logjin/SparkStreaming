package test

import java.util

/**
  * Kafka工具类
  *
  * @author huangfeifei
  */
object KafkaUtils {

  /**
    * 消息转换
    *
    * @param msg
    * @return
    */
  def transformMessage(msg: String): util.ArrayList[String] = {
    val fieldValueList = new util.ArrayList[String]()
    val msgArr = msg.split("\",\"")
    for (i <- 1 until (msgArr.length - 1)) {
      fieldValueList.add(msgArr(i))
    }
    // 最后一个字段
    fieldValueList.add(msgArr(msgArr.length - 1).replace("\"", "").replace("'", ""))
    fieldValueList
  }
}
