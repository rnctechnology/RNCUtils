package com.rnctech.common.utils

import scala.collection.Seq
import com.rnctech.common.NRDataType._

/**
 * Table meta data
 */

case class SourceTableKey(keyName: String, keyType: KeyType, columnNames: List[String], tableName: String, schemaName: String = "")
case class SourceIndex(idxname: String, indexType: String, columnNames: List[String], tableName: String, schemaName: String = "", isUnique: Boolean = false, ascOrDesc: List[String] = List.empty)
case class EntityColumn(name: String , dataType: String = "STRING", sqlType: Int = 12, var length: Integer = null,  var scale: Integer = null, defValue: String = null, allowNull: Boolean = true, var accuracy: Double = EntityColumn.DEFAULT_DECIMAL_TYPE, description: String)
object EntityColumn {
  final val DEFAULT_DECIMAL_TYPE: Double = 38.5
  final val DECIMAL_DEFAULT_ACCURACY: Double = 38.5
  final val MIN_SCALE: Int = 2
}

case class SourceEntity (name: String, instance: String,  attributes: Seq[EntityColumn] = Seq(), keys: Seq[SourceTableKey] = Seq(), indices: Seq[SourceIndex] = Seq(),
    tablePrefix: String = "", description: String = null,  tableType: SourceTableType = SourceTableType.TABLE, tableProperties: String = "") {

  def fullName: String = if (instance == null) tablePrefix + name else instance + "."+tablePrefix + name

  def toDescribeStyleString: String = {
    if (attributes.isEmpty) {
      return "table " + name + " doesn't have any columns"
    }

    def withoutCRLF(str: String): String = {
      if (str == null) null
      else str.replaceAll("\r", "").replaceAll("\n", "")
    }
    def length(str: String): Int = {
      if (str == null) 0
      else withoutCRLF(str).map(c => if (c.toString.getBytes.length > 1) 2 else 1).sum
    }
    def take(str: String, maxLength: Int): String = {
      if (str == null) null
      else withoutCRLF(str).foldLeft(("", 0)) {
        case ((str, len), c) =>
          if (len >= maxLength) (str, len)
          else if (len + length(c.toString) > maxLength) (str, maxLength)
          else (str + c, len + length(c.toString))
      }._1
    }

    "\n" +
      "Table: " + fullName + {
        if (description == null || description.trim.length == 0) ""
        else if (length(description) > 64) " (" + take(description, 62) + "..)"
        else " (" + withoutCRLF(description) + ")"
      } + "\n" +
      attributes.map { column =>
        val typeName = column.dataType + (if (column.length > 0) "(" + column.length + ")" else "")
        val nullable = if (column.allowNull) "YES" else "NO"
        val defaultValue = {
          if (column.defValue == null) "NULL"
          else if (column.defValue.length > 20) column.defValue.take(18) + ".."
          else column.defValue
        }
        val description = {
          if (column.description == null) ""
          else if (length(column.description) > 40) take(column.description, 38) + ".."
          else column.description
        }

        "| " + column.name +  " | " +
          typeName + " | " +
          nullable + " | " +
          defaultValue + " | " +
          column.sqlType +  " | " +
          withoutCRLF(description) + " |\n"
      }.mkString +
      {
        if (indices.nonEmpty) "Indexes:\n" + indices.map { index => "  \"" + index.idxname + "\"" + (if (index.isUnique) " UNIQUE," else "") + " (" + index.columnNames.mkString(", ") + ")" + "\n" }.mkString
        else ""
      } + {
        if (keys.nonEmpty) "Keys:\n" + keys.map { fk => "  " + fk.keyName + " -> " + fk.keyType + "(" + fk.tableName + " " +fk.columnNames+")" + "\n" }.mkString
        else ""
      }
  }

}
