package com.rnctech.writers.kafka

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.util.zip.GZIPOutputStream
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.io.{BinaryEncoder, EncoderFactory}
import org.apache.avro.specific.SpecificDatumWriter

class AvroWriter(val schema : Schema, extraData : Array[Byte] = Array()) {
  val writer = new SpecificDatumWriter[GenericRecord](schema)
  //Converters.addConverters(writer.getData)
  val out = new ByteArrayOutputStream()
  val gzipout = new GZIPOutputStream(out)
  val encoder: BinaryEncoder = {
    val enc = EncoderFactory.get().binaryEncoder(gzipout, null)
    val extraDataWriter = new SpecificDatumWriter[ByteBuffer](Schema.create(Schema.Type.BYTES))
    extraDataWriter.write(ByteBuffer.wrap(extraData), enc)
    val schemaWriter = new SpecificDatumWriter[String](Schema.create(Schema.Type.STRING))
    schemaWriter.write(schema.toString, enc)
    enc
  }
  def createRecord = new GenericData.Record(schema)
  val initialSize: Int = out.size
  def write(data : GenericData.Record): Unit = {
    writer.write(data, encoder)
  }

  def write(data : Array[Object]): Unit = {
    val record = new GenericData.Record(schema)
    //for(i <- data.indices) record.put(i, data(i))
    for(i <- data.indices)
      data(i) match {
        case s: java.lang.Short => record.put(i, new Integer(s.intValue()))
        case v => record.put(i, v)
      }
    writer.write(record, encoder)
  }

  def close : Array[Byte] = {
    encoder.flush()
    gzipout.close()
    out.close()
    out.toByteArray
  }
  def getCurrentSize: Int = encoder.bytesBuffered() + out.size() - initialSize

}

object AvroWriter {
  def apply(schema : Schema, extraData : Array[Byte] = Array()) =
    new AvroWriter(schema, extraData)

  def apply(schema : String) =
    new AvroWriter(new Schema.Parser().parse(schema))

}
