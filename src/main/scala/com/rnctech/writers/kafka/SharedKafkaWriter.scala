package com.rnctech.writers.kafka

import java.io.IOException
import java.util.Properties
import java.util.concurrent.TimeUnit
import org.apache.avro.Schema
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import scala.collection.JavaConverters._

class SharedKafkaWriter(private val brokers : String*) extends KafkaWriterBase(brokers : _*) {

  protected def this(brokers : Array[String]) = this(brokers.toSeq : _*)

  protected def this(brokers : java.util.List[String]) = this(brokers.asScala.toSeq : _*)

  def writeRecords(topic : String, key: String, schema: Schema, extraData : Array[Byte], data : java.util.Iterator[Array[Object]]) : Unit =
    writeIterRecordsToKafka(topic, key, schema, extraData, data match {
      case null => _ : Unit =>  null
      case  _   => _ : Unit => data.asScala
    }
    )

  def writeRecords(topic : String, key: String, schema: Schema, extraData : Array[Byte], data : java.lang.Iterable[Array[Object]]) : Unit =
    writeIterRecordsToKafka(topic, key, schema, extraData, data match {
      case null => _ : Unit =>  null
      case  _   => _ : Unit => data.iterator.asScala
    }
    )

  def writeRecords(topic : String, key: String, schema: Schema, extraData : Array[Byte],  data: => Stream[Array[Object]]): Unit = {
    super.writeRecordsToKafka(topic, key, schema, extraData, _ => data)
  }

  def writeRecords(topic : String, key: String, schema: Schema, extraData : Array[Byte], data: Unit => Stream[Array[Object]]): Unit = {
    super.writeRecordsToKafka(topic, key, schema, extraData, data)
  }

}
