package com.rnctech.common.utils

import java.util
import com.rnctech.common.NRDataSink
import scala.collection.JavaConverters._

class UpdateProgressMessage(val entities: util.Map[String, util.Set[String]], val status: NRDataSink.STATUS, val progress: Double, val message: String, val stackTrace: Array[StackTraceData]) {
  def this(entities: util.Map[String, util.Set[String]], status: NRDataSink.STATUS, progress: Double, message: String, stackTrace: util.List[StackTraceData]) {
    this(entities, status, progress, message, Option(stackTrace).map(_.asScala.toArray).orNull)
  }
  def this(entities: util.Map[String, util.Set[String]], status: NRDataSink.STATUS, progress: Double, message: String, stackTrace: Array[StackTraceElement]) {
    this(entities, status, progress, message, Option(stackTrace).map(_.map(new StackTraceData(_))).orNull)
  }
  def this(entities: util.Map[String, util.Set[String]], status: NRDataSink.STATUS, progress: Double, message: String) {
    this(entities, status, progress, message, null.asInstanceOf[Array[StackTraceData]])
  }
}