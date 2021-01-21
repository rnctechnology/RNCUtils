package com.rnctech.common.utils

import java.sql.{Time, Timestamp}
import java.util.Date
import java.lang.{Long => jLong}

import org.apache.avro.Conversions.DecimalConversion
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.{Conversion, LogicalType, LogicalTypes, Schema}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import MetaConverters._
import org.apache.avro.Schema.Field
import org.apache.spark.sql.types.{StructField, StructType}

import scala.annotation.tailrec
import scala.collection.JavaConverters._

object Converters {
  def addConverters(data  :GenericData): GenericData = {
    Seq(DateConverter, TimestampConverter, DecimalConversion, SqlDateConverter, TimeConverter,
      DateConverterSqlDate, DateConverterSqlTime, DateConverterSqlTimestamp)
      .foreach(data.addLogicalTypeConversion)
    data
  }

  protected class DateConverterBase extends Conversion[Date] {
    override def toLong(value: Date, schema: Schema, `type`: LogicalType): jLong = value.getTime
    override def fromLong(value: jLong, schema: Schema, `type`: LogicalType): Date = new Date(value)
    override def getConvertedType: Class[Date] = classOf[Date]
    override def getLogicalTypeName: String = LogicalTypes.timeMicros().getName
    //override def getLogicalTypeName: String = LogicalTypes.timestampMillis().getName
  }

  object DateConverter extends DateConverterBase

  object DateConverterSqlTimestamp extends DateConverterBase {
    override def toLong(value: Date, schema: Schema, `type`: LogicalType): jLong = value.getTime
    override def fromLong(value: jLong, schema: Schema, `type`: LogicalType): Date = new Date(value)
    override def getConvertedType: Class[Date] = classOf[Date]
    override def getLogicalTypeName: String = LogicalTypes.timestampMicros().getName
    //override def getLogicalTypeName: String = LogicalTypes.timestampMillis().getName
  }

  object DateConverterSqlDate extends DateConverterBase {
    override def toLong(value: Date, schema: Schema, `type`: LogicalType): jLong = value.getTime
    override def fromLong(value: jLong, schema: Schema, `type`: LogicalType): Date = new Date(value)
    override def getConvertedType: Class[Date] = classOf[Date]
    override def getLogicalTypeName: String = SqlDateType.getName
    //override def getLogicalTypeName: String = LogicalTypes.timestampMillis().getName
  }

  object DateConverterSqlTime extends DateConverterBase {
    override def toLong(value: Date, schema: Schema, `type`: LogicalType): jLong = value.getTime
    override def fromLong(value: jLong, schema: Schema, `type`: LogicalType): Date = new Date(value)
    override def getConvertedType: Class[Date] = classOf[Date]
    override def getLogicalTypeName: String = SqlTimeType.getName
    //override def getLogicalTypeName: String = LogicalTypes.timestampMillis().getName
  }

  object TimeConverter extends Conversion[Time] {
    override def toLong(value: Time, schema: Schema, `type`: LogicalType): jLong = value.getTime
    override def fromLong(value: jLong, schema: Schema, `type`: LogicalType): Time = new Time(value)
    override def getConvertedType: Class[Time] = classOf[Time]
    override def getLogicalTypeName: String = SqlTimeType.getName
    //override def getLogicalTypeName: String = LogicalTypes.timestampMillis().getName
  }

  object SqlDateConverter extends Conversion[java.sql.Date] {
    override def toLong(value: java.sql.Date, schema: Schema, `type`: LogicalType): jLong = value.getTime
    override def fromLong(value: jLong, schema: Schema, `type`: LogicalType): java.sql.Date = new java.sql.Date(value)
    override def getConvertedType: Class[java.sql.Date] = classOf[java.sql.Date]
    override def getLogicalTypeName: String = SqlDateType.getName
    //override def getLogicalTypeName: String = LogicalTypes.timestampMillis().getName
  }

  object TimestampConverter extends Conversion[Timestamp] {
    override def toLong(value: Timestamp, schema: Schema, `type`: LogicalType): jLong = value.getTime
    override def fromLong(value: jLong, schema: Schema, `type`: LogicalType): Timestamp = new Timestamp(value)
    override def getConvertedType: Class[Timestamp] = classOf[Timestamp]
    override def getLogicalTypeName: String = LogicalTypes.timestampMicros().getName
  }

  object DecimalConversion extends DecimalConversion


  def convertData(row: GenericRecord, columns : Seq[ColumnSchema], defaultValues : Map[String, Any], upconvert : Boolean): Array[Any] = {
    def findField(name : String): Int = {
      val encodedName = encodeAsHex(name)
      @tailrec
      def findField(idx: Int, fields: List[Field]): Int =
        fields match {
          case head :: Nil => if (head.name == encodedName || head.name == name)
            idx
          else
            -1
          case head :: tail => if (head.name == encodedName || head.name == name)
            idx
          else
            findField(idx + 1, tail)
          case Nil => -1
        }
      findField(0, row.getSchema.getFields.asScala.toList)
    }

    columns.zipWithIndex
      .map{col =>
        findField(col._1.name) -> col._1
      }
      .map{it =>
        val colType = it._2.`type`.toUpperCase
        Option(
        it._1 match {
          case -1 => null
          case i =>
            row.get(i) match {
              case string: org.apache.avro.util.Utf8 =>
                val value = string.toString
                val len = colType match {
                  case "TEXT" =>
                    0xFFFF
                  case _ =>
                    it._2.accuracy.asInstanceOf[Option[_]].map {
                      case v: Int => v
                      case v: Double => v.toInt
                    }.filter(0 != _).getOrElse(40)
                }
                if (len < value.length)
                  value.substring(0, len)
                else
                  value

              case value if null != value && upconvert && (colType == "STRING" || colType == "TEXT" || colType == "VARCHAR" || colType == "CHAT") => value.toString
              case date: Date => new Timestamp(date.getTime)
              case value : java.lang.Integer if upconvert => if(colType == "DECIMAL") new java.math.BigDecimal(value.longValue()) else new java.lang.Long(value.longValue())
              case value : java.lang.Number if upconvert && colType == "DECIMAL" => new java.math.BigDecimal(value.longValue())
              case value => value
            }
        }).getOrElse(defaultValues.getOrElse(it._2.name, null))
      }
    .toArray
  }

  implicit def asRow(row: GenericRecord)(implicit schema : TableSchema, skippedColumns : Set[String]) : GenericRowWithSchema =
    toRow(row, schema.columns.filter(c=> !skippedColumns.contains(c.name)))

  implicit def toRow(row: GenericRecord, columns : Seq[ColumnSchema]) : GenericRowWithSchema = toRow(row, columns, Map.empty)

  implicit def toRow(row: GenericRecord, columns : Seq[ColumnSchema], defaultValues : Map[String, Any], upconvert : Boolean = false) : GenericRowWithSchema =
    new GenericRowWithSchema(
      convertData(row, columns, defaultValues, upconvert),
      StructType(columns.map(field=>
          StructField(field.shortName.getOrElse(field.name),
            TableSchema.mapColumnType(field.`type`, field.accuracy), nullable = true
          )
        )
      )
    )

}
