package com.rnctech.common.utils

import java.sql._
import scala.reflect.ClassTag
import java.sql.Types
import java.util.HashMap
import java.util.Properties
import scala.io.Source
import scala.collection.mutable.ArraySeq
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.reflect.FieldUtils
import scala.collection.JavaConverters._
import scala.collection.JavaConversions._

/**
* Created by Alan on 3/30/2018.
*/

class TypeMapper {
  
  val DT_MAPPING_PREFIX = "attributeMapping."
  val DATATYPE_MAPPING_PROP: String = "/attributeMapping.properties"
  val PROP_DEF_TIMEZONE: String = "defaultTimeZoneQuery"
  val PROP_TEST_CNN: String = "testQuery"
  val jdbcTypeNames = new HashMap[Integer, String]()  
  val configuration: Properties = new Properties()
  val url = getClass.getResource(DATATYPE_MAPPING_PROP)
  val fs = FieldUtils.getAllFieldsList(classOf[java.sql.Types]) 
    
  def init(dbProduct: DBProduct) {
     if (url != null) {
      val source = Source.fromURL(url)
      val prop: Properties = new Properties()
      prop.load(source.bufferedReader())
      prop.keySet().asScala.filter(it=>it.asInstanceOf[String].startsWith(DT_MAPPING_PREFIX+dbProduct.name))
          .foreach(k => {
            var nk = k.asInstanceOf[String].substring((DT_MAPPING_PREFIX+dbProduct.name).length()+1)
            configuration.put(nk, prop.get(k))})
    }
     if(0 == jdbcTypeNames.size())
      fs.asScala.toList.foreach(f => {
  				var name = f.getName();
  				var value: Integer = f.get(null).asInstanceOf[Integer];
  				jdbcTypeNames.put(value, name); 
      })  
  }
  
  def clear() {
    configuration.clear
  }
  
  def set(stmt: PreparedStatement, index: Int, value: Any): Unit = {
    value match {
      case x: Int       => stmt.setInt(index, x)
      case x: Integer   => stmt.setInt(index, x)
      case x: Long      => stmt.setLong(index, x)
      case x: Double    => stmt.setDouble(index, x)
      case x: Short     => stmt.setShort(index, x)
      case x: Float     => stmt.setFloat(index, x)
      case x: Timestamp => stmt.setTimestamp(index, x)
      case x: Date      => stmt.setDate(index, x)
      case x: Time      => stmt.setTime(index, x)
      case x: String    => stmt.setString(index, x)
      case _ => throw new UnsupportedOperationException(s"Unsupported type: ${value.getClass.getName}")
    }
  }

  def get[T](rs: ResultSet, index: Int, jtype: Int): T = {
    (if(jtype == Types.INTEGER) {
      rs.getInt(index)
    } else if(jtype == Types.BIT || jtype == Types.BOOLEAN) {
      rs.getBoolean(index)
    } else if(jtype == Types.BIGINT) {
      rs.getLong(index)
    } else if(jtype == Types.SMALLINT) {
      rs.getShort(index)
    } else if(jtype == Types.DECIMAL || jtype == Types.NUMERIC) {
      rs.getBigDecimal(index)
    } else if(jtype == Types.DOUBLE) {
      rs.getDouble(index)
    } else if(jtype == Types.FLOAT) {
      rs.getFloat(index)
    } else if(jtype == Types.TIMESTAMP) {
      rs.getTimestamp(index)
    } else if(jtype == Types.DATE) {
      rs.getDate(index)
    } else if(jtype == Types.TIME) {
      rs.getTime(index)
    } else if(jtype == Types.VARCHAR || jtype == Types.CHAR){
      rs.getString(index)
    } else {
      rs.getString(index)
    }).asInstanceOf[T]
  }
  
  def get[T](rs: ResultSet, index: Int)(implicit m: ClassTag[T]): T = {
    val c = m.runtimeClass
    (if(c.isAssignableFrom(classOf[Int]) || c.isAssignableFrom(classOf[Integer])) {
      rs.getInt(index)
    } else if(c.isAssignableFrom(classOf[Long])) {
      rs.getLong(index)
    } else if(c.isAssignableFrom(classOf[Double])) {
      rs.getDouble(index)
    } else if(c.isAssignableFrom(classOf[Float])) {
      rs.getFloat(index)
    } else if(c.isAssignableFrom(classOf[Short])) {
      rs.getShort(index)
    } else if(c.isAssignableFrom(classOf[Timestamp])) {
      rs.getTimestamp(index)
    } else if(c.isAssignableFrom(classOf[Date])) {
      rs.getDate(index)
    } else if(c.isAssignableFrom(classOf[Time])) {
      rs.getTime(index)
    } else if(c.isAssignableFrom(classOf[String])){
      rs.getString(index)
    } else {
      throw new UnsupportedOperationException(s"Unsupported type: ${c.getName}")
    }).asInstanceOf[T]
  }
  
  def getJdbcTypeName(typeSpecifier: Int): String = {
		 if(jdbcTypeNames.containsKey(typeSpecifier)) {
		   jdbcTypeNames.get(typeSpecifier)
		 }else{
		   "unknown"
		 }
	}
	
	def getJDBCMetadata(dbProduct: DBProduct, jdbcTypeSpecifier: Integer, databaseType: String, length: Integer, scale: Integer): Seq[Integer] = {
		val jdbcmeta = ArraySeq(jdbcTypeSpecifier, length, scale)
		dbProduct match{
		  case `oracle` => {
		    if (databaseType.equalsIgnoreCase("NUMBER")) {	    		  
    			 if(scale <= 0) {    			  
    			  if(length ==0){
    			    jdbcmeta(0) =  Types.DECIMAL  //Oracle Number default as (0, -127)
    			  }else if(length < 5) {
    					jdbcmeta(0) = Types.SMALLINT
    				}else if(length < 9) {
    					jdbcmeta(0) =  Types.INTEGER
    				}else if(length < 19) {
    					jdbcmeta(0) =  Types.BIGINT
    				}
    				
    				if(length < 19) { 
    					jdbcmeta(1) = 0
    					jdbcmeta(2) = -1
    				}else {
    					jdbcmeta(0) =  Types.DECIMAL
    					jdbcmeta(1) = length + 2
    					jdbcmeta(2) = 2
    				}
    			}else if(length < scale){ //handle oracle Number(2,5) 
    				jdbcmeta(1) = scale + 1
    			}
    		}else if(databaseType.equalsIgnoreCase("FLOAT")){ //Oracle float allow as real or double
    		   jdbcmeta(0) =  Types.DECIMAL
    		   jdbcmeta(1) = 38
    			 jdbcmeta(2) = 5
    		} else if(jdbcTypeSpecifier == -102){
  	      jdbcmeta(0) =  Types.TIMESTAMP
    		   jdbcmeta(1) = 0
    			 jdbcmeta(2) = -1 
    		}
		  }
		  case _ => 
		}
		
		if(length < scale) jdbcmeta(1) = (Math.min(EntityColumn.DEFAULT_DECIMAL_TYPE, 1+scale)).asInstanceOf[Int]
		if(0 > scale) jdbcmeta(2) = 0
		
		jdbcmeta
	}		
	
	def getXmlType(dbProduct: DBProduct, jdbcTypeSpecifier: Integer, databaseType: String, length: Integer = -1, scale: Integer = -1): String = {
	  var xtype: String = configuration.get(databaseType) match {
	    case null => "STRING"
	    case _ => (configuration.get(databaseType).asInstanceOf[String]).toUpperCase()
	  }
	  dbProduct match {
	    case `oracle` => 	{
	      if (0 < length && databaseType.equalsIgnoreCase("NUMBER") && scale <= 0) {
    			if(length < 5){
    				xtype = "SHORT"
    			}else if(length < 9){
    				xtype = "INTEGER"
    			}else if(length < 19){
    				xtype = "LONG"
    			}else{
    				xtype = "DECIMAL"
    			}
  	    } else if(databaseType.equalsIgnoreCase("FLOAT")){
  	      xtype = "DECIMAL"
  	    } else if(jdbcTypeSpecifier == -102){
  	      xtype = "DATETIME" 
  	    }
		  }
	    case `informix` => {
			  val xmlType: String = configuration.get(getJdbcTypeName(jdbcTypeSpecifier)).asInstanceOf[String]
			  if (StringUtils.containsIgnoreCase(xmlType, "dateTime")) // Informix seems to return TIMESTAMP types even when the day part is missing or the time part is missing.
				  xtype = "STRING"
			  else
			     xtype = xmlType.toUpperCase()
		  } 
	    case `sqlserver` => {
			  val typeQualifiers: ArraySeq[String]  = StringUtils.split(databaseType, StringUtils.SPACE).asInstanceOf[ArraySeq[String]]
  			var dtype = typeQualifiers(0)
			  if ((typeQualifiers.length > 1) && (StringUtils.contains(typeQualifiers(0), "unsigned"))) {
  				dtype = typeQualifiers(1) // unsigned smallint, unsigned int and unsigned bigint
  			} 
  			xtype = (configuration.get(dtype).asInstanceOf[String]).toUpperCase();
  		} 
	    case _ => 
	  }
	  xtype
	}
	
	def populateLength(dbProduct: DBProduct, jdbcTypeSpecifier: Integer): Boolean = {
	  dbProduct match {
	    case `oracle` => {
	      if(jdbcTypeSpecifier == Types.CHAR || jdbcTypeSpecifier == Types.CLOB || jdbcTypeSpecifier == Types.LONGNVARCHAR || 
	         jdbcTypeSpecifier == Types.LONGVARCHAR || jdbcTypeSpecifier == Types.NCHAR || jdbcTypeSpecifier == Types.NVARCHAR || 
	         jdbcTypeSpecifier == Types.VARCHAR || jdbcTypeSpecifier == Types.BLOB || jdbcTypeSpecifier == Types.NCLOB || 
	         jdbcTypeSpecifier == Types.LONGVARBINARY || jdbcTypeSpecifier == Types.VARBINARY)
	        true
	      else
	        false
	    }
	    case _ => {
	      if(jdbcTypeSpecifier == Types.CHAR || jdbcTypeSpecifier == Types.CLOB || jdbcTypeSpecifier == Types.LONGNVARCHAR || 
	         jdbcTypeSpecifier == Types.LONGVARCHAR || jdbcTypeSpecifier == Types.NCHAR || jdbcTypeSpecifier == Types.NVARCHAR || 
	         jdbcTypeSpecifier == Types.VARCHAR)
	        true
	      else
	        false
	    }
	  }
	}	
	
	def populateScale(jdbcTypeSpecifier: Int): Boolean = {
    jdbcTypeSpecifier match {
		  case Types.REAL  => true
		  case Types.JAVA_OBJECT => true
		  case Types.DOUBLE => true
		  case Types.FLOAT => true
		  case Types.DECIMAL => true
		  case Types.NUMERIC => true
		  case _ => false
		} 
	}
	  
  def getTimeZoneQuery(): String = {
    configuration.get(PROP_DEF_TIMEZONE).asInstanceOf[String]
  }
  
  def getTestConnQuery(): String = {
    configuration.get(PROP_TEST_CNN).asInstanceOf[String]
  }
  
  
}