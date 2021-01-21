package com.rnctech.writers.kafka

import java.util.Properties
import java.util.concurrent.TimeUnit
import org.apache.avro.Schema
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import com.rnctech.common.utils.Logging
import com.rnctech.writers.StructuredWriter
import scala.annotation.tailrec
import scala.collection.mutable
import com.rnctech.common.exception.WriterException

class KafkaWriterBase (private val brokers : String*) extends Logging with AutoCloseable {
  private final val MESSAGE_SIZE = Option(System.getProperty("max.request.size")).map(_.toInt).getOrElse(1000000)

  private def createProducer = try {
    val props = new Properties()
    props.put("bootstrap.servers", brokers.mkString(","))
    props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
    props.put("retries", 3.asInstanceOf[AnyRef])
    props.put("max.request.size", MESSAGE_SIZE.asInstanceOf[AnyRef])
    new KafkaProducer[Array[Byte], Array[Byte]](props)
  } catch {
    case th : Throwable =>
      val message = String.join("Brokers:", brokers.mkString(","))
      reportError(th, message)
  }

  private def reportError(th: Throwable, message: String) = {
    error(s"$message due to ${th.getMessage}", th)
    throw new WriterException(message, th)
  }

  private val producer = createProducer

  protected def sendMessage(topic : String, key : String, writer : AvroWriter): RecordMetadata = {
    val producerRecord = new ProducerRecord[Array[Byte], Array[Byte]](topic, key.getBytes("UTF-8"), writer.close)
    val size = java.lang.Long.valueOf(writer.getCurrentSize)
    debug(String.join("send message",  topic,  key))
    val res = producer.send(producerRecord)
    try {
      val meta = res.get(1, TimeUnit.MINUTES)
      debug(String.join("Send "+ producerRecord.value().length.asInstanceOf[Integer], topic, key))
      meta
    } catch {
      case th : Throwable =>
        val message = String.join("Error with size="+size, topic, key)
        reportError(th, message)
    }
  }

  protected def sendMessage(topic : String, key : String): Unit = {
    val producerRecord = new ProducerRecord[Array[Byte], Array[Byte]](topic, key.getBytes("UTF-8"),null)
    producer.send(producerRecord)
  }

  protected def writeRecordsToKafka(topic : String, key: String, schema: Schema, extraData : Array[Byte], data: Unit => Stream[Array[Object]]): Unit = {

    data match {
      case null => sendMessage(topic, key)
      case _    => data().iterator match {
        case null => sendMessage(topic, key)
        case dataStream if dataStream.nonEmpty => writeIterRecordsToKafka(topic, key, schema, extraData, Unit => dataStream.toIterator)
        case dataStream =>
          Unit
      }
    }
    producer.flush()
  }

  protected def writeIterRecordsToKafka(topic : String, key: String, schema: Schema, extraData : Array[Byte], data: Unit => Iterator[Array[Object]]): Unit = {
    debug("start write topic " + topic+" key as "+ key)
    @tailrec def write(data : Iterator[Array[Object]], count : Int, writer : AvroWriter = AvroWriter(schema, extraData), prevSize : Int = 0, recSize : Int = 0) : Unit = {
      if (data.hasNext) {
        val truncated = data.next().map(_ match {
            case s: String if s.length >= 0x10000 => s.substring(0, 0x10000)
              case v                                => v
        }
        )
        writer.write(truncated)
        val newSize = writer.getCurrentSize
        val maxRecSize = Math.max(recSize, newSize - prevSize)
        if (writer.getCurrentSize + maxRecSize > ((MESSAGE_SIZE * 9) / 10)) {
          sendMessage(topic, key, writer)
          write(data, count + 1)
        } else {
          write(data, count + 1, writer, newSize, maxRecSize)
        }
      } else {
        if (writer.getCurrentSize > 0) {
          sendMessage(topic, key, writer)
        }
        debug("count: "+ count.asInstanceOf[AnyRef] + " topic=" +topic+" key="+ key)
      }

    }

    data match {
      case null => sendMessage(topic, key)
      case _    => data() match {
          case null => sendMessage(topic, key)
          case iter => write(iter, 0)
        }
    }
    producer.flush()
  }

  private var closed = false
  override def close(): Unit = synchronized {
    if(!closed) {
      producer.flush()
      producer.close()
      closed = true
    }
  }

}
