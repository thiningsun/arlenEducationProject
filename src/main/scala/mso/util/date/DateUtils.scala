package mso.util.date

import java.util.Calendar

import org.apache.commons.lang3.time.FastDateFormat

class DateUtils {
  var parameter: String = _

  val cal = Calendar.getInstance()
  val fastdateDay = FastDateFormat.getInstance("yyyy-MM-dd")

  /**
    * 带入的参数形如 2017-05-25
    */
  def this(parameter: String) {
    this()
    this.parameter = parameter
    cal.set(getCalYear().toInt, getCalMonth().toInt - 1, getCalDay().toInt)
  }

  def getCalYear(): String = {
    parameter.substring(0, 4)
  }

  def getCalMonth(): String = {
    parameter.substring(5, 7)
  }

  def getCalDay(): String = {
    parameter.substring(8, 10)
  }

  def getSubNinetyDay(): String = {
    cal.add(Calendar.DATE, -90)
    val result = fastdateDay.format(cal)
    cal.add(Calendar.DATE, 90)
    result
  }

  def getSubHalfOfYear(): String = {
    cal.add(Calendar.DATE, -180)
    val result = fastdateDay.format(cal)
    cal.add(Calendar.DATE, 180)
    result
  }

  def getSubSixtyDay(): String = {
    cal.add(Calendar.DATE, -60)
    val result = fastdateDay.format(cal)
    cal.add(Calendar.DATE, 60)
    result
  }

  def getFourteenDay(): String = {
    cal.add(Calendar.DATE, -14)
    val result = fastdateDay.format(cal)
    cal.add(Calendar.DATE, 14)
    result
  }
}
