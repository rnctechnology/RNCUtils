package com.rnctech.common.utils

import scala.language.implicitConversions
import java.sql.{ Date => sqlDate, Time => sqlTime, Timestamp => sqlTimestamp }
import java.util.{ Date => utilDate,  Calendar, TimeZone}
import java.text.{SimpleDateFormat, DateFormat}
import org.joda.time.format.{DateTimeFormatter, ISODateTimeFormat}

/**
 * Implicit conversions for date/time/time stamp values.
 * Created by Alan Chen on 4/18/2020.
 */

trait DateTimeUtil {
  final val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
  final val stdFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  implicit def utcTimeConverter(): UTCTimeConverter = new UTCTimeConverter()
}

class UTCTimeConverter() extends DateTimeUtil {

  val calendar: Calendar = Calendar.getInstance()   
	dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"))
	
	def toSString(date: utilDate): String = stdFormat.format(date)
  
  def toUTCString(d: utilDate = calendar.getTime()): String = {    
	    dateFormat.format(d);
  }

  def toUTCTimestamp: sqlTimestamp = {
    new java.sql.Timestamp(calendar.getTimeInMillis)
  }
  
  def getEpochTime(): String = {
    val dateTimeFormatter: DateTimeFormatter = ISODateTimeFormat.dateHourMinuteSecond().withZoneUTC();
    dateTimeFormatter.print(0)
	}
  
  @throws[Exception]
  def getTimeDelta(lastTime: String, delta: String, epoch: String): String = {
    var let = lastTime.replace("T"," ")
    val withDelta: Long = getTimeInMillisec(let) - delta.toLong
    var res: String = stdFormat.format(withDelta)
	  res.replace(" ","T")
	  if(res < epoch) res = epoch	  
	  res
	}
        
  @throws[Exception]
  def getTimeInMillisec(dateTime: String): Long =  {
    var resultInMillis: Long = 0L
    stdFormat.parse(dateTime).getTime()
  }

}