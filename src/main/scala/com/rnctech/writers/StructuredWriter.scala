package com.rnctech.writers

import org.apache.avro.Schema
import scala.collection.JavaConverters._

abstract trait StructuredWriter extends AutoCloseable {

  def writeRecords(key: String, schema: Schema, data: java.util.Iterator[Array[Object]]): Unit =
    writeRecords(key, schema, data.asScala.toStream, Array[Byte]())

  def writeRecords(key: String, schema: Schema, data: List[Array[Object]]): Unit =
    writeRecords(key, schema, data.toStream, Array[Byte]())

  def writeRecords(key: String, schema: Schema, data: java.util.Iterator[Array[Object]], extraData: Array[Byte]): Unit =
    writeRecords(key, schema, data.asScala.toStream, extraData)

  def writeRecords(key: String, schema: Schema, data: List[Array[Object]], extraData: Array[Byte]): Unit =
    writeRecords(key, schema, data.toStream, extraData)

  def writeRecords(key: String, schema: Schema, data: => Stream[Array[Object]], extraData: Array[Byte] = Array()): Unit

}
