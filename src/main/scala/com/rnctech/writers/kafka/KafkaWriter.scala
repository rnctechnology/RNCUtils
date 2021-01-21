package com.rnctech.writers.kafka

import java.util.Properties
import org.apache.avro.Schema
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import java.io.IOException
import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters._
import com.rnctech.writers.StructuredWriter

class KafkaWriter(private val topic : String, brokers : String*) extends KafkaWriterBase(brokers: _*) with StructuredWriter {

  protected def this(topic : String, brokers : Array[String]) = this(topic, brokers.toSeq : _*)

  protected def this(topic : String, brokers : java.util.List[String]) = this(topic, brokers.asScala.toSeq : _*)

  override def writeRecords(key: String, schema: Schema, data: => Stream[Array[Object]], extraData : Array[Byte]): Unit = {
    super.writeRecordsToKafka(topic, key, schema, extraData, _ => data)
  }
}
