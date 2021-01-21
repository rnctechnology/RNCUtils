package com.rnctech.common.utils

import java.time.{Duration, Instant}
import org.slf4j.{LoggerFactory, MDC}
import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future};

trait Logging { self=>
  lazy val logger = LoggerFactory.getLogger(self.getClass)
  def debug(msg : String) = logger.debug(msg)
  
  def info(msg : String) = logger.info(msg)
  def trace(msg : String) = logger.trace(msg)
  def warn(msg : String) = logger.warn(msg)
  def error(msg : String) = logger.error(msg)
  def error(str : String, th : Throwable) = logger.error(str, th)

  def FutureWithLogging[T](info : Map[String, String])(block : => T)(implicit ctx : ExecutionContext): Future[T] = Future{
    withLogging(info)(block)
  }

  def withLogging[T](info : Map[String, String])(block : => T): T = {
    val oldValues = Option(MDC.getCopyOfContextMap)
    try {
      info.foreach(it => MDC.put(it._1, it._2))
      block
    } finally {
      info.map(_._1).foreach(MDC.remove(_))
      try {
        oldValues.foreach(_.asScala.foreach(it => MDC.put(it._1, it._2)))
      } catch {
        case th : Throwable =>
          error(th.getMessage, th)
      }
    }
  }

  def withTimeLogging[T](message : String) (block : => T): T = {
    val start = Instant.now
    try {
      block
    } finally {
      info(String.format(message, Duration.between(start, Instant.now)))
    }
  }

}
