package com.atguigu.streaming


import java.util.Date

import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

/**
  * 日期处理工具类
  *
  * @author huangfeifei
  */
object DateUtils {

  val MILLISECOND_FORMAT_PATTERN = "yyyyMMddHHmmssSSS"
  val SECOND_FORMAT_PATTERN = "yyyyMMddHHmmss"
  val DAY_FORMAT_PATTERN = "yyyyMMdd"

  val DEFAULT_FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
  val GS_LWSF_EXIT_INSERTTIME_FORMAT: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS")


  /**
    * 字符串转化成Date，默认使用GS_LWSF_EXIT_INSERTTIME_FORMAT
    *
    * @param s
    * @return
    */
  def parseDate(s: String): Date = DateTime.parse(s, GS_LWSF_EXIT_INSERTTIME_FORMAT).toDate

  /**
    * 字符串转化成Date，以指定的pattern
    *
    * @param pattern
    * @param s
    * @return
    */
  def parseDate(pattern: String, s: String): Date = DateTime.parse(s, DateTimeFormat.forPattern(pattern)).toDate

  /**
    * 将日期转化为固定格式的字符串
    *
    * @param date the date to format.
    * @return a string like "yyyy-MM-dd HH:mm:ss".
    */
  def dateFormat(date: Date): String = new DateTime(date).toString(DEFAULT_FORMATTER)

  /**
    * 将日期转化为指定格式的字符串
    *
    * @param pattern specify a pattern.
    * @param date    the date to format.
    * @return the string after format.
    */
  def dateFormat(pattern: String, date: Date): String = new DateTime(date).toString(pattern)

  /**
    * 计算当前日期d天前的日期
    *
    * @param d       天数
    * @param pattern 格式
    * @return
    */
  def dateBeforeFormat(d: Int, pattern: String): String = {
    DateTime.now.minusDays(d).toString(pattern)
  }

  /**
    * 计算当前日期d天后的日期
    *
    * @param d       天数
    * @param pattern 格式
    * @return
    */
  def dateAfterFormat(d: Int, pattern: String): String = {
    DateTime.now.plusDays(d).toString(pattern)
  }
}
