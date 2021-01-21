package com.rnctech.common.utils

import java.sql.Types
import java.util.Optional
import java.{lang, util}
import org.apache.spark.sql.types._
import scala.collection.JavaConverters._
 
case class ColumnSchema(name : String, `type` : String, accuracy : Option[Double], skip : Boolean = false, shortName: Option[String] = None)

case class TableSchema(table : String, index: Int, purgeData : Boolean, columns : Seq[ColumnSchema], sourceInstance: Int = 0, keys : Seq[String] = Seq.empty, parent: String = null, tablePrefix : String = null, skiptypecheck : Boolean = false) {
  def this(table : String, columns : Seq[ColumnSchema]) = this(table, -1, false, columns)
}

object TableSchema {
  def mapColumnType(name: String, accuracy: Option[_]): DataType = name.toUpperCase match {
    case "CHAR" => StringType
    case "VARCHAR" => StringType
    case "STRING" => StringType
    case "LONG" => LongType
    case "BIGINT" => LongType
    case "SMALLINT" => LongType
    case "INTEGER" => LongType
    case "INT" => IntegerType
    case "DOUBLE" => DoubleType
    case "FLOAT" => FloatType
    case "REAL" => FloatType
    case "DATETIME" => TimestampType
    case "DATE" => TimestampType
    case "TIMESTAMP" => TimestampType
    case "BOOLEAN" => BooleanType
    case "BIT" => BooleanType
    case "ENUM" => StringType
    case "DECIMAL" => DecimalType(
      accuracy match {
        case Some(v) => v match {
          case 0 => 38
          case n: Int => n
          case d: Double => d.toInt
        }
        case None => 38
      },
      accuracy match {
        case Some(20.0) => 10
        case Some(0) => 5
        case Some(v : Integer) => 0
        case Some(v : Double) => Math.round((v - v.toInt) * 10).toInt
        case None => 5
        case _ => 5
      }
    )
    case _ => StringType
  }

  private def getPrecision(accuracy: Option[_]) = accuracy match {
    case Some(v) => v match {
      case n: Int => n
      case d: Double => d.toInt
    }
    case None => 0
  }

  def mapColumnTypeToAvro(`type`: String, accuracy: Option[_]): (Int, Int, Int) = `type`.toUpperCase match {
    case "CHAR" => (Types.VARCHAR, getPrecision(accuracy), 0)
    case "VARCHAR" => (Types.VARCHAR, getPrecision(accuracy), 0)
    case "STRING" => (Types.VARCHAR, getPrecision(accuracy), 0)
    case "TEXT" => (Types.VARCHAR, getPrecision(accuracy), 0)
    case "BIGINT" => (Types.BIGINT, 0, 0)
    case "SMALLINT" => (Types.SMALLINT, 0, 0)
    case "SHORT" => (Types.INTEGER, 0, 0)
    case "INTEGER" => (Types.INTEGER, 0, 0)
    case "LONG" => (Types.BIGINT, 0, 0)
    case "INT" => (Types.INTEGER, 0, 0)
    case "DATETIME" => (Types.TIMESTAMP, 0, 0)
    case "DATE" => (Types.DATE, 0, 0)
    case "TIME" => (Types.TIME, 0, 0)
    case "TIMESTAMP" => (Types.TIMESTAMP, 0, 0)
    case "BOOLEAN" => (Types.BIT, 0, 0)
    case "BIT" => (Types.BIT, 0, 0)
    case "ENUM" => (Types.VARCHAR, 0, 0)
    case "FLOAT" => (Types.FLOAT, 0, 0)
    case "DOUBLE" => (Types.DOUBLE, 0, 0)
    case "DECIMAL" => (Types.DECIMAL,
      accuracy match {
        case Some(v) => v match {
          case 0 => 38
          case n: Int => n
          case d: Double => d.toInt
        }
        case None => 38
      },
      accuracy match {
        case Some(20.0) => 10
        case Some(0) => 5
        case Some(v : Integer) => 0 //{if(v < 5) 0 else 5 }
        case Some(v : Double) => Math.round((v - v.toInt) * 10).toInt
        case None => 5
        case _ => 5
      }
    )
    case _ => (Types.VARCHAR, 0, 0)
  }

  def mapColumnTypeToJDBC(`type`: String, accuracy: Option[_]): Int =
    mapColumnTypeToAvro(`type`, accuracy)._1

  
  def isColumnExist(entity: String, schema : TableSchema): Boolean = {
    if(null == entity || 0 == entity.length()) false
    0 != schema.columns.filter(cs => entity.equalsIgnoreCase(cs.name)).size
  }

  def toShortVersion(schema : TableSchema) =
    TableSchema(schema.table, schema.index, schema.purgeData,
      schema.columns.map(col => ColumnSchema(col.shortName.getOrElse(col.name), col.`type`, col.accuracy, col.skip ))
      , schema.sourceInstance, schema.keys.map(col=>schema.columns.find(_.name == col).flatMap(_.shortName).getOrElse(col)), schema.parent, schema.tablePrefix, schema.skiptypecheck)

  def toShortVersion(name : String, lookupCols : Seq[ColumnSchema]) : String =
        lookupCols.find(_.name == name).flatMap(_.shortName).getOrElse(name)
}

