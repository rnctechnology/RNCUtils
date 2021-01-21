package com.rnctech.common.utils

import java.sql._
import org.apache.avro.LogicalTypes.{TimeMicros, TimeMillis, TimestampMicros}
import org.apache.avro.Schema.{Field, Type}
import org.apache.avro.{LogicalType, LogicalTypes, Schema}
import org.apache.avro.reflect.ReflectData
import org.apache.spark.sql.types._
import scala.collection.JavaConverters._
import scala.util.Random


object MetaConverters  {

    private final val dateSchema: Schema = LogicalTypes.timeMicros().addToSchema(Schema.create(Type.LONG))

    private final val sqldateSchema: Schema = SqlDateType.addToSchema(Schema.create(Type.LONG))

    private final val sqltimeSchema: Schema = SqlTimeType.addToSchema(Schema.create(Type.LONG))

    private final val timestampSchema: Schema = LogicalTypes.timestampMicros().addToSchema(Schema.create(Type.LONG))

    private final val bytesSchema = Schema.create(Type.BYTES)

    private final val stringSchema = Schema.create(Type.STRING)

    private final val intSchema = Schema.create(Type.INT)

    private final val longSchema = Schema.create(Type.LONG)

    private final val floatSchema = Schema.create(Type.FLOAT)

    private final val doubleSchema = Schema.create(Type.DOUBLE)

    private final val booleanSchema = Schema.create(Type.BOOLEAN)

    private final val nullSchema = Schema.create(Type.NULL)

    final val PLAIN_DATE = java.sql.Types.TIMESTAMP + 1

    private def decimalSchema(precision : Int, scale : Int) = LogicalTypes.decimal(precision, scale).addToSchema(Schema.create(Type.BYTES))

    private def varcharSchema(length : Int) = new Varchar(length).addToSchema(Schema.create(Type.STRING))


    class Varchar(val length : Int) extends LogicalType("varchar") {
      def this(schema : Schema) {
        this(schema.getObjectProp("length").asInstanceOf[java.lang.Number].intValue())
      }
      override def addToSchema(schema: Schema): Schema = {
        super.addToSchema(schema)
        schema.addProp("length", length)
        schema
      }

    }
    private final val METADATA_COLUMN_NAME = 4
    private final val METADATA_COLUMN_TYPE = 5
    private final val METADATA_PRECISION = 7
    private final val METADATA_SCALE = 9
    private final val METADATA_NULLABLE = 11

  object SqlDateType extends LogicalType("date-micros")

  object SqlTimeType extends LogicalType("time-micros")

  object VarcharType extends LogicalType("varchar")

  LogicalTypes.register(SqlDateType.getName, new LogicalTypes.LogicalTypeFactory {
    override def fromSchema(schema: Schema): LogicalType = SqlDateType
  })

  LogicalTypes.register(SqlTimeType.getName, new LogicalTypes.LogicalTypeFactory {
    override def fromSchema(schema: Schema): LogicalType = SqlTimeType
  })

  LogicalTypes.register(VarcharType.getName, new LogicalTypes.LogicalTypeFactory {
    override def fromSchema(schema: Schema): LogicalType = new Varchar(schema)
  })



  implicit def toSchema(con : Connection, catalog:String, schema:String, table:String) : Schema =
      toSchema(con.getMetaData.getColumns(catalog, schema, table, "%"), Some(table))

    implicit def toSchema(rs : ResultSetMetaData) : Schema = toSchema(rs, None)

    implicit def toSchema(rs : ResultSetMetaData, name : String) : Schema = toSchema(rs, Some(name))

    implicit def toSchema(rs : ResultSetMetaData, name : Option[String]) : Schema = {
      toCompleteNullableSchema(
        (for(idx <- 1.to(rs.getColumnCount)) yield
          (rs.getColumnName(idx), rs.getColumnType(idx),
            true, rs.getPrecision(idx), rs.getScale(idx)
          )
        ).toStream, name)
    }

    implicit def toSchema(rs : ResultSet, name : Option[String]) : Schema = {
      def rowStream : Stream[(String, Int, Boolean, Int, Int)] =
        if (rs.next())
          (rs.getString(METADATA_COLUMN_NAME), rs.getLong(METADATA_COLUMN_TYPE).toInt,
            DatabaseMetaData.columnNoNulls != rs.getInt(METADATA_NULLABLE),
            Option(rs.getObject(METADATA_PRECISION)).map(_ => rs.getInt(METADATA_PRECISION)).getOrElse(0),
            Option(rs.getObject(METADATA_SCALE)).map(_ => rs.getInt(METADATA_SCALE)).getOrElse(0)) #:: rowStream
        else
          Stream.empty
      toCompleteNullableSchema(rowStream, name)
    }

    implicit def toSchemaFromJDBC(t: JDBCType, nullable: Boolean = false, precision: Int = 20, scale: Int = 0): Schema = {
      val schema = t match {
        case JDBCType.BOOLEAN                 => booleanSchema
        case JDBCType.BIT                     => booleanSchema
        case JDBCType.TINYINT                 => intSchema
        case JDBCType.SMALLINT                => intSchema
        case JDBCType.INTEGER                 => intSchema
        case JDBCType.BIGINT                  => longSchema
        case JDBCType.FLOAT                   => floatSchema
        case JDBCType.DOUBLE                  => doubleSchema
        case JDBCType.CHAR                    => if(0 == precision) stringSchema else varcharSchema(precision)
        case JDBCType.LONGVARCHAR             => if(0 == precision) stringSchema else varcharSchema(precision)
        case JDBCType.VARCHAR                 => if(0 == precision) stringSchema else varcharSchema(precision)
        case JDBCType.NCHAR                   => if(0 == precision) stringSchema else varcharSchema(precision)
        case JDBCType.LONGNVARCHAR            => if(0 == precision) stringSchema else varcharSchema(precision)
        case JDBCType.NVARCHAR                => if(0 == precision) stringSchema else varcharSchema(precision)
        case JDBCType.NCLOB                   => stringSchema
        case JDBCType.CLOB                    => stringSchema
        case JDBCType.BINARY                  => bytesSchema
        case JDBCType.VARBINARY               => bytesSchema
        case JDBCType.LONGVARBINARY           => bytesSchema
        case JDBCType.BLOB                    => bytesSchema
        case JDBCType.DATE                    => sqldateSchema
        case JDBCType.TIME                    => sqltimeSchema
        case JDBCType.TIME_WITH_TIMEZONE      => sqltimeSchema
        case JDBCType.TIMESTAMP               => timestampSchema
        case JDBCType.TIMESTAMP_WITH_TIMEZONE => timestampSchema
        case JDBCType.DECIMAL                 => decimalSchema(precision, scale)
        case JDBCType.NULL                    => nullSchema
        case JDBCType.NUMERIC                 => decimalSchema(precision, scale)
        case _ => stringSchema
      }
      if(nullable && schema != nullSchema)
        ReflectData.makeNullable(schema)
      else
        schema
    }

    implicit def toSchema(t: Int, nullable: Boolean = false, precision: Int = 20, scale: Int = 0): Schema = {
      val schema = t match {
        case java.sql.Types.BIT                     => booleanSchema
        case java.sql.Types.BOOLEAN                 => booleanSchema
        case java.sql.Types.TINYINT                 => intSchema
        case java.sql.Types.SMALLINT                => intSchema
        case java.sql.Types.INTEGER                 => intSchema
        case java.sql.Types.BIGINT                  => longSchema
        case java.sql.Types.FLOAT                   => floatSchema
        case java.sql.Types.DOUBLE                  => doubleSchema
        case java.sql.Types.CHAR                    => if(0 == precision) stringSchema else varcharSchema(precision)
        case java.sql.Types.LONGVARCHAR             => if(0 == precision) stringSchema else varcharSchema(precision)
        case java.sql.Types.VARCHAR                 => if(0 == precision) stringSchema else varcharSchema(precision)
        case java.sql.Types.CLOB                    => stringSchema
        case java.sql.Types.BINARY                  => if(0 == precision) stringSchema else varcharSchema(precision)
        case java.sql.Types.VARBINARY               => if(0 == precision) stringSchema else varcharSchema(precision)
        case java.sql.Types.LONGVARBINARY           => if(0 == precision) stringSchema else varcharSchema(precision)
        case java.sql.Types.BLOB                    => bytesSchema
        case java.sql.Types.DATE                    => sqldateSchema
        case java.sql.Types.TIME                    => sqltimeSchema
        case java.sql.Types.TIME_WITH_TIMEZONE      => sqltimeSchema
        case java.sql.Types.TIMESTAMP               => timestampSchema
        case java.sql.Types.TIMESTAMP_WITH_TIMEZONE => timestampSchema
        case java.sql.Types.NULL                    => nullSchema
        case PLAIN_DATE                             => dateSchema
        case java.sql.Types.DECIMAL                 => decimalSchema(precision, scale)
        case java.sql.Types.NUMERIC                 => decimalSchema(precision, scale)  
        case _ => stringSchema
      }
      if(nullable && schema != nullSchema)
        ReflectData.makeNullable(schema)
      else
        schema
    }

    implicit def toSchema(columns : => Stream[(String, Int, Boolean)]): Schema = {
      val schema = Schema.createRecord(s"___${Math.abs(Random.nextLong())}____", null, null, false)
      schema.setFields(
        columns.map(f=>new Field(f._1,toSchema(f._2, f._3), null, null.asInstanceOf[Object])).toList.asJava
      )
      schema
    }

    implicit def toJdbcSchema(columns : => Stream[(String, JDBCType, Boolean)]): Schema = {
      val schema = Schema.createRecord(s"___${Math.abs(Random.nextLong())}____", null, null, false)
      schema.setFields(
        columns.map(f=>new Field(f._1,toSchemaFromJDBC(f._2, f._3), null, null.asInstanceOf[Object])).toList.asJava
      )
      schema
    }

    implicit def toCompleteSchema(columns : => Iterable[(String, Int, Boolean, Int, Int)], name : Option[String] = None): Schema = {
      val schema = Schema.createRecord (name match {
        case Some(n) => encodeAsHex(n)
        case None => s"___${Math.abs(Random.nextLong())}____"
      }, null, null, false)
      schema.setFields(
        columns.map(f=>new Field(encodeAsHex(f._1),toSchema(f._2, f._3, f._4, f._5), null, null.asInstanceOf[Object])).toList.asJava
      )
      schema
    }

    implicit def toCompleteJdbcSchema(columns : => Iterable[(String, JDBCType, Boolean, Int, Int)], name : Option[String] = None): Schema = {
      val schema = Schema.createRecord (name match {
        case Some(n) => encodeAsHex(n)
        case None => s"___${Math.abs(Random.nextLong())}____"
      }, null, null, false)
      schema.setFields(
        columns.map(f=>new Field(encodeAsHex(f._1),toSchemaFromJDBC(f._2, f._3, f._4, f._5), null, null.asInstanceOf[Object])).toList.asJava
      )
      schema
    }

    implicit def toNullableSchema(columns : => Stream[(String, Int)]): Schema =
      toSchema(columns.map(column=>(column._1, column._2, true)))

    implicit def toCompleteNullableSchema(columns : => Iterable[(String, Int, Boolean, Int, Int)], name : Option[String] = None): Schema =
      toCompleteSchema(columns,name)

    implicit def toSchema(struct : StructType): Schema = {
      def mapField(field : StructField) : (String, Int, Boolean, Int, Int)= (
        field.name,
        field.dataType match {
          case StringType     => java.sql.Types.VARCHAR
          case FloatType      => java.sql.Types.FLOAT
          case DoubleType     => java.sql.Types.DOUBLE
          case BooleanType    => java.sql.Types.BIT
          case IntegerType    => java.sql.Types.INTEGER
          case LongType       => java.sql.Types.BIGINT
          case NullType       => java.sql.Types.NULL
          case DateType       => java.sql.Types.DATE
          case TimestampType  => java.sql.Types.TIMESTAMP
          case DecimalType()  => java.sql.Types.DECIMAL
          case _              => java.sql.Types.NULL
        },
        field.nullable,
        field.dataType match {
          case d : DecimalType  => d.precision
          case _                => 0
        },
        field.dataType match {
          case d : DecimalType  => d.scale
          case _                => 0
        }
      )
      toCompleteNullableSchema(struct.fields.toStream
        .map(field=>mapField(field)),
        Some(struct.typeName)
      )
    }

    implicit def toStruct(schema: Schema) : StructType = {
      def mapColumnType(field : Schema): (DataType, Boolean) = field.getType match {
        case Schema.Type.STRING => (StringType, false)
        case Schema.Type.INT    => (IntegerType, false)
        case Schema.Type.LONG   => field.getLogicalType match {
          case null => (LongType, false)
          case    _ => (TimestampType, false)
        }
        case Schema.Type.BOOLEAN=> (BooleanType, false)
        case Schema.Type.FLOAT  => (FloatType, false)
        case Schema.Type.DOUBLE => (DoubleType, false)
        case Schema.Type.NULL   => (NullType, false)
        case Schema.Type.UNION  =>
          (field.getTypes.asScala.find(mapColumnType(_)._1!=NullType).map(mapColumnType).map(_._1).getOrElse(NullType),
            field.getTypes.asScala.exists(mapColumnType(_)._1==NullType))
        case _ if null != field.getLogicalType => field.getLogicalType match {
          case _ : TimestampMicros  => (TimestampType, false)
          case _ : TimeMicros       => (DateType, false)
          case SqlDateType          =>  (DateType, false)
          case SqlTimeType          =>  (DateType, false)
          case t : org.apache.avro.LogicalTypes.Decimal => (DecimalType(t.getPrecision, t.getScale), false)
        }
        case _                  => (StringType, false)
      }
      StructType(schema.getFields.asScala.map(field=> {
        val (dt, nullable) = mapColumnType(field.schema)
        StructField(decodeFromHex(field.name), dt, nullable)
      }))
    }

    implicit def toStruct(schema: Schema, nameMapper : String => String, columnFilter : String => Boolean = _ => true) : StructType = {
      def mapColumnType(field : Schema): (DataType, Boolean) = field.getType match {
        case Schema.Type.STRING => (StringType, false)
        case Schema.Type.INT    => (IntegerType, false)
        case Schema.Type.LONG   => field.getLogicalType match {
          case null => (LongType, false)
          case    _ => (TimestampType, false)
        }
        case Schema.Type.BOOLEAN=> (BooleanType, false)
        case Schema.Type.FLOAT  => (FloatType, false)
        case Schema.Type.DOUBLE => (DoubleType, false)
        case Schema.Type.NULL   => (NullType, false)
        case Schema.Type.UNION  =>
          (field.getTypes.asScala.find(mapColumnType(_)._1!=NullType).map(mapColumnType).map(_._1).getOrElse(NullType),
            field.getTypes.asScala.exists(mapColumnType(_)._1==NullType))
        case _ if null != field.getLogicalType => field.getLogicalType match {
          case _ : TimestampMicros  => (TimestampType, false)
          case _ : TimeMicros       => (DateType, false)
          case SqlDateType          =>  (DateType, false)
          case SqlTimeType          =>  (DateType, false)
          case t : org.apache.avro.LogicalTypes.Decimal => (DecimalType(t.getPrecision, t.getScale), false)
        }
        case _                  => (StringType, false)
      }
      StructType(schema.getFields.asScala.map(field=> {
        val (dt, nullable) = mapColumnType(field.schema)
        Option(
        StructField(nameMapper(decodeFromHex(field.name)), dt, nullable)).filter(col=>columnFilter(col.name))
      }).filter(_.isDefined).map(_.get))
    }

    def mapColumnType(field : Schema): Int = field.getType match {
      case Schema.Type.STRING => Types.VARCHAR
      case Schema.Type.INT    => Types.INTEGER
      case Schema.Type.LONG   => field.getLogicalType match {
        case null => Types.BIGINT
        case    _ => Types.TIMESTAMP
      }
      case Schema.Type.BOOLEAN=> Types.BIT
      case Schema.Type.FLOAT  => Types.FLOAT
      case Schema.Type.DOUBLE => Types.DOUBLE
      case Schema.Type.NULL   => Types.NULL
      case Schema.Type.UNION  => field.getTypes.asScala
        .find(mapColumnType(_)!=Types.NULL)
        .map(mapColumnType).getOrElse(Types.NULL)
      case _ if null != field.getLogicalType => field.getLogicalType match {
        case _ : TimestampMicros  => Types.TIMESTAMP
        case _ : TimeMicros       => Types.DATE
        case SqlDateType          =>  Types.DATE
        case SqlTimeType          =>  Types.TIME
        case _ : org.apache.avro.LogicalTypes.Decimal => Types.DECIMAL
      }
      case _                  => Types.VARCHAR
    }

    def mapColumnTypeWithLength(field : Schema): (Int, Int, Int) = field.getType match {
      case Schema.Type.STRING => field.getLogicalType match {
          case varchar : Varchar    =>  (Types.VARCHAR,varchar.length, 0)
          case null                 =>  (Types.VARCHAR,0, 0)
      }
      case Schema.Type.INT    => (Types.INTEGER,0,0)
      case Schema.Type.LONG   => field.getLogicalType match {
        case null => (Types.BIGINT,0,0)
        case    _ => (Types.TIMESTAMP,0,0)
      }
      case Schema.Type.BOOLEAN=> (Types.BIT,0,0)
      case Schema.Type.FLOAT  => (Types.FLOAT,0,0)
      case Schema.Type.DOUBLE => (Types.DOUBLE,0,0)
      case Schema.Type.NULL   => (Types.NULL,0,0)
      case Schema.Type.UNION  => field.getTypes.asScala
        .find(mapColumnTypeWithLength(_)._1!=Types.NULL)
        .map(mapColumnTypeWithLength).getOrElse((Types.NULL,0,0))
      case _ if null != field.getLogicalType =>
        field.getLogicalType match {
        case _ : TimestampMicros  => (Types.TIMESTAMP,0,0)
        case _ : TimeMicros       => (Types.DATE,0,0)
        case SqlDateType          => (Types.DATE,0,0)
        case SqlTimeType          => (Types.TIME,0,0)
        case dec : org.apache.avro.LogicalTypes.Decimal => (Types.DECIMAL, dec.getPrecision, dec.getScale)
      }
      case _                  => (Types.VARCHAR,0, 0)
    }

    def mapColumnTypeWithLengthAndNullability(field : Schema): (Int, Int, Int, Boolean) = field.getType match {
      case Schema.Type.STRING => field.getLogicalType match {
        case varchar : Varchar    =>
          (Types.VARCHAR,varchar.length, 0, false)
        case null => (Types.VARCHAR,0, 0, false)
      }
      case Schema.Type.INT    => (Types.INTEGER,0,0, false)
      case Schema.Type.LONG   => field.getLogicalType match {
        case null => (Types.BIGINT,0,0, false)
        case    _ => (Types.TIMESTAMP,0,0, false)
      }
      case Schema.Type.BOOLEAN=> (Types.BIT,0,0, false)
      case Schema.Type.FLOAT  => (Types.FLOAT,0,0, false)
      case Schema.Type.DOUBLE => (Types.DOUBLE,0,0, false)
      case Schema.Type.NULL   => (Types.NULL,0,0, false)
      case Schema.Type.UNION  =>
        field.getTypes.asScala
          .find(mapColumnTypeWithLength(_)._1!=Types.NULL)
          .map(mapColumnTypeWithLength).map(it=>
          (it._1, it._2, it._3, field.getTypes.asScala.exists(_.getType == Type.NULL ))
        ).getOrElse((Types.NULL,0,0,false))
      case _ if null != field.getLogicalType => field.getLogicalType match {
        case _ : TimestampMicros  => (Types.TIMESTAMP,0,0,false)
        case _ : TimeMicros       => (Types.DATE,0,0,false)
        case SqlDateType          => (Types.DATE,0,0,false)
        case SqlTimeType          => (Types.TIME,0,0,false)
        case dec : org.apache.avro.LogicalTypes.Decimal => (Types.DECIMAL, dec.getPrecision, dec.getScale,false)
        case varchar : Varchar    => (Types.VARCHAR,varchar.length, 0,false)
      }
      case _                  => (Types.VARCHAR,0, 0,false)
    }

    def toTypes(schema: Schema) : List[Int] = {
      schema.getFields.asScala.map(field=>mapColumnType(field.schema)).toList
    }

    private final val HEX = scala.Array('0','1','2','3','4','5','6','7','8','9','a','b','c','d','e','f')
    private final val HEX_IDX = HEX.zipWithIndex.map(it=>(it._1, it._2.toByte)).toMap

    def encodeAsHex(string : String): String =
      "_"+string.getBytes("UTF-8").flatMap(byte => scala.Array(byte&0xF, (byte>>4)&0xF)).map(HEX(_)).map(_.toString).reduce(_ + _)
    def decodeFromHex(string : String): String = {
      if(string.startsWith("_") && 1 == (string.length %2) && string.toCharArray.tail.forall(HEX_IDX.contains)) {
        val string1 = string.substring(1)
        new String((for(i <-  1.to(string.length / 2)) yield
          (HEX_IDX(string.charAt(i * 2 - 1)) | (HEX_IDX(string.charAt(i * 2)) << 4)).toByte
        ).toArray, "UTF-8")
      } else {
        string
      }
    }

    def getFieldType(columnType :  (Int, Int, Int, Boolean)) : (Int, String, Double, Boolean) = {
      val (colType, len, scale, nullable) = columnType
      colType match {
        case Types.VARCHAR if len < 255 => (colType,  "STRING",   if(len > 0) len else 40, nullable)
        case Types.VARCHAR              => (colType,  "TEXT",     0, nullable)
        case Types.DOUBLE               => (colType,  "DOUBLE",   0, nullable)
        case Types.FLOAT                => (colType,  "FLOAT",    0, nullable)
        case Types.DECIMAL              => (colType,  "DECIMAL",  len + 0.1 * (if(scale > 9) 0.9 else 0.1 * scale), nullable)
        case Types.INTEGER              => (colType,  "INTEGER",  0, nullable)
        case Types.BIGINT               => (colType,  "BIGINT",   0, nullable)
        case Types.BIT                  => (colType,  "BOOLEAN",  0, nullable)
        case Types.BOOLEAN              => (colType,  "BOOLEAN",  0, nullable)
        case Types.DATE                 => (colType,  "DATE",     0, nullable)
        case Types.TIME                 => (colType,  "TIME",     0, nullable)
        case Types.TIMESTAMP            => (colType,  "DATETIME", 0, nullable)
      }
    }

    def getFieldType(fieldSchema : Schema, nullable : Boolean = false) : (String, Int, Int, Boolean) = {
      Option(fieldSchema.getLogicalType).map {_  match {
          case varchar: Varchar => ("STRING", varchar.length, 0, nullable)
          case decimal: LogicalTypes.Decimal => ("DECIMAL", decimal.getPrecision, decimal.getScale, nullable)
          case _: LogicalTypes.TimestampMicros => ("TIMESTAMP", 0, 0, nullable)
          case SqlDateType => ("DATE", 0, 0, nullable)
          case SqlTimeType => ("TIME", 0, 0, nullable)
        }
      }.getOrElse {
        fieldSchema.getType match {
          case Type.UNION => getFieldType(fieldSchema.getTypes.asScala.filter(_.getType != Type.NULL).head, true)
          case Type.STRING => ("STRING", 0, 0, nullable)
          case Type.INT => ("INT", 0, 0, nullable)
          case Type.LONG => ("BIGINT", 0, 0, nullable)
          case Type.FLOAT => ("FLOAT", 0, 0, nullable)
          case Type.DOUBLE => ("DOUBLE", 0, 0, nullable)
          case Type.BOOLEAN => ("BOOLEAN", 0, 0, nullable)
          case Type.BYTES => ("VARBINARY", 0, 0, nullable)
          case _ => ("STRING", 0, 0, nullable)
        }
      }
    }

    implicit def toTableSchema(schema : Schema) : TableSchema = {
      val name = decodeFromHex(schema.getName)
      TableSchema(name, 0, false, schema.getFields.asScala.map { field =>
        val fieldName = decodeFromHex(field.name())
        val typeData = getFieldType(field.schema())
        ColumnSchema(fieldName, typeData._1, Option(typeData._2 + typeData._3/10.0), typeData._4)
      })
    }
}