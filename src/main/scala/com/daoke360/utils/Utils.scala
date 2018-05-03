package com.daoke360.utils

import java.text.SimpleDateFormat
import java.util.regex.Pattern

/**
  * Created by Luo on 2018/5/3.
  */
object Utils {
  /**
    * 将时间戳转换成指定格式的 日期
    *
    * @param longTime
    * @param pattern
    * @return
    */
  def formatDate(longTime: Long, pattern: String) = {
    val sdf = new SimpleDateFormat(pattern)
    val calendar = sdf.getCalendar
    calendar.setTimeInMillis(longTime)
    sdf.format(calendar.getTime)
  }

  /**
    * 将指定格式的日期转换成long类型的时间戳
    *
    * @param inputDate
    * @param pattern
    */
  def parseDate2Long(inputDate: String, pattern: String) = {
    val sdf = new SimpleDateFormat(pattern)
    val date = sdf.parse(inputDate)
    date.getTime
  }


  /**
    * 验证日期是否是yyyy-MM-dd这种格式
    *
    * @param inputDate
    * @return
    */
  def validateInpuDate(inputDate: String) = {
    val reg = "^((((1[6-9]|[2-9]\\d)\\d{2})-(0?[13578]|1[02])-(0?[1-9]|[12]\\d|3[01]))|(((1[6-9]|[2-9]\\d)\\d{2})-(0?[13456789]|1[012])-(0?[1-9]|[12]\\d|30))|(((1[6-9]|[2-9]\\d)\\d{2})-0?2-(0?[1-9]|1\\d|2[0-8]))|(((1[6-9]|[2-9]\\d)(0[48]|[2468][048]|[13579][26])|((16|[2468][048]|[3579][26])00))-0?2-29-))$"
    val pattern = Pattern.compile(reg)
    pattern.matcher(inputDate).matches()
  }


  /**
    * 将nginx服务器时间转换成long类型的时间
    *
    * @param nginxTime 1522284954.558
    * @return 1522284954558
    */
  def nginxTime2Long(nginxTime: String): Long = {
    (nginxTime.toDouble * 1000).toLong
  }
}

