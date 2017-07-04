package com.lakala.finance.stream.screen.common

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.Logging

/**
  * Created by longxiaolei on 2017/2/20.yy
  */
object DateTimeUtils extends Logging {
  private val dayFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
  private val secondDateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  private val testDataFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

  def getCurrentTime(): String = {
    val date: Date = new Date()
    secondDateFormat.format(date)
  }

  def getCurrentDay(): String = {
    dayFormat.format(new Date())
  }

  def getSecondTimStr(timeStamp: Long): String = {
    secondDateFormat.format(new Date(timeStamp))
  }

  /**
    * 将时间戳转换成对应的yyyy-MM-dd 格式的字符串
    *
    * @param timeStamp
    * @return
    */
  def getDayStr(timeStamp: Long): String = {
    dayFormat.format(new Date(timeStamp))
  }

  def getYesterdayStr(dayStr: String) = {
    val date: Date = dayFormat.parse(dayStr)
    val cal: Calendar = Calendar.getInstance()
    cal.setTime(date)
    cal.add(Calendar.DAY_OF_MONTH, -1)
    dayFormat.format(cal.getTime)
  }

  def getTimeStampOfStr(timeStr: String) = {
    secondDateFormat.parse(timeStr).getTime
  }

  /**
    * 获取该时间对应的1分钟间隔的时间
    *
    * @param timeStamp
    * @return
    */
  def getIntervalTimeStr(timeStamp: Long): Long = {
    //一分钟
    //    val cal: Calendar = Calendar.getInstance()
    //    cal.setTimeInMillis(timeStamp)
    //    cal.set(Calendar.SECOND, 0)
    //    cal.set(Calendar.MILLISECOND, 0)
    //    cal.getTimeInMillis

    //两分钟
    val cal: Calendar = Calendar.getInstance()
    cal.setTimeInMillis(timeStamp)
    cal.set(Calendar.SECOND, 0)
    cal.set(Calendar.MILLISECOND, 0)
    var minute: Int = cal.get(Calendar.MINUTE)
    minute = minute / 2 * 2 + 2
    cal.set(Calendar.MINUTE,minute)
    cal.getTimeInMillis

    //5分钟
    //    val cal: Calendar = Calendar.getInstance()
    //    cal.setTimeInMillis(timeStamp)
    //    val year: Int = cal.get(Calendar.YEAR)
    //    val dayOfYear: Int = cal.get(Calendar.DAY_OF_YEAR)
    //    val hourOfDay: Int = cal.get(Calendar.HOUR_OF_DAY)
    //    var minute: Int = cal.get(Calendar.MINUTE)
    //    minute = minute / 5 * 5 + 5 //将分钟数转换成对应的5分钟间隔时间
    //    cal.clear()
    //    cal.set(Calendar.YEAR, year)
    //    cal.set(Calendar.DAY_OF_YEAR, dayOfYear)
    //    cal.set(Calendar.HOUR_OF_DAY, hourOfDay)
    //    cal.set(Calendar.MINUTE, minute)
    //    cal.getTimeInMillis
  }

  def main(args: Array[String]): Unit = {
    val str: Long = getIntervalTimeStr(new Date().getTime)
    print(testDataFormat.format(new Date(str)))
  }
}
