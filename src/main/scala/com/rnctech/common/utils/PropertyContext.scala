package com.rnctech.common.utils

import java.util.Properties
import java.util.regex.Pattern
import java.sql.Types
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.util.{Random, Success, Failure, Try}
import com.rnctech.common.FieldData
import com.rnctech.common.Constants

/**
 * Context of configuration or key-value pairs
 * Created by Alan on 4/17/2020.
 */

case class EntityProp(name: String, instance: String, schema: TableSchema, prefix: String, val let: String, letcolumn: String = null, cdcpurge: Boolean = false, cdcdel: String = null, cdcdelexpr: String = null, query: String = null, owcolumns: Map[String, FieldData]= Map.empty)

class PropertyContext(val config: Map[String, Map[String, Any]], instance: String, dbcfg: DBConfig, fullprop: Boolean = true, pentity: Set[String] = Set.empty) extends DateTimeUtil with JsonUtil {

 
  val instprop = config.get(instance).getOrElse(Map.empty)
  val runningInstances = config.get("Entities").getOrElse(Map.empty)
  var entities = runningInstances(instance).asInstanceOf[Set[String]]
  if(!pentity.isEmpty) entities = pentity
  val entityNames = entities.mkString(",")
  val random = Random
  final val randomid = f"${java.time.Instant.now.getEpochSecond * 1000 + java.time.Instant.now.getNano / 1000000}%d.${Random.nextInt(Integer.MAX_VALUE)}%08x"
  val INIT_EPOCH = utcTimeConverter().getEpochTime
  val sampleCount: Int = 100
  val jobid = jobProperties.get("JobKey").getOrElse("-1");
    
  final val entityProps: Map[String, EntityProp] = {
    val safetyDelta = "300000"  //millisecond
     entities.map{
       entity => {
           val econfig: Map[String, Any] = config.get(entity).getOrElse(Map.empty)
           val let = getEntityProp(econfig, entity, "let")
           val epoch = getEntityProp(econfig, entity, "elet", INIT_EPOCH)
           val safelet = utcTimeConverter().getTimeDelta(let, safetyDelta, epoch)    
           var letcol = getEntityProp(econfig, entity, "source.rdbms.letcol")
           val query= getProp(econfig, entity, "source.rdbms.letexpr", "source.rdbms.letexpr")   
           var eprop = EntityProp(entity, instance, null, "", safelet, letcol, false, null,  null, query, Map.empty)
           
           fullprop match {
             case true => {
               val schema: TableSchema = readJsonString[TableSchema](econfig.get("Schema").asInstanceOf[String])
               val tblprefix = schema.tablePrefix
               val cdcpurge: Boolean = false;
               val cdcdel = getProp(econfig, entity, "source.rdbms.delcol", "cdc.deletedColumn")      
               val cdcdelexpr = getProp(econfig, entity, "source.rdbms.delexpr", "cdc.deletedExpression")                           
               eprop = EntityProp(entity, instance, schema, tblprefix, safelet, letcol, cdcpurge, cdcdel,  cdcdelexpr, query)
             }
             case false => 
           }
           (entity, eprop)
       }
     }.toMap           
  }
  
  def getInstanceProp(key: String, default: String = ""): String =
    instprop.get(key).getOrElse(default).asInstanceOf[String]
    
  def getEntityProp(econfig: Map[String, Any], entity: String, key: String, default: String = ""): String = 
    Option(econfig.getProperty(key)).filter(!_.trim.isEmpty).getOrElse(default)
    
  def getProp(econfig: Map[String, Any], entity: String, ekey: String,  ikey: String, default: String = ""): String = 
    Option(econfig.getProperty(ekey)).getOrElse(getInstanceProp(ikey, default))  
    
  def resolve(props: Stream[(String, Any)]): Stream[(String, Any)] =
    props.map(it => it._1 -> (it._2 match {
      case null | "" => if (defaultProperties.contains(it._1)) defaultProperties(it._1) else null
      case v         => v
    }))
  
  
  
  lazy val defaultProperties: Map[String, Any] = Seq(
    "tunnel.properties",
    "s3Client.properties").map(Thread.currentThread().getContextClassLoader.getResourceAsStream).map(is => {
      val props = new Properties
      props.load(is)
      props.asScala.toMap
    }).reduce(_ ++ _)

  final val props = defaultProperties ++ (jobProperties).filter(null != _._2).toMap

  def instanceProperties: Stream[(String, Any)] = {    
    Stream(  
      "DBType" -> dbcfg.dbProduct.name,
      "DBTimezone" -> dbcfg.dbservertimezone,
      "DataJobType" -> jobProperties.get("jobType").getOrElse(Constants.DataJobType.SAMPLE.toString()),
      "jobid" -> jobid,
      "sampleCount" -> sampleCount,
      "entityNames" -> entityNames
  )}
 
  def getEntityProperties(entityNames : String, propKey: String, isTS: Boolean)(implicit econfig : Map[String, Any]) : String = {
    var letList = new ListBuffer[String]()
    entityNames.split(",").foreach { 
        entity => var let = Option(econfig.getProperty(propKey)).getOrElse("")
        if(isTS) let = let.replaceAll("\\s","T")
        letList += let
    }
    letList.mkString(",")
  }

 implicit def toProperties(map: Map[String, Any]): Properties = {
    val props = new Properties
    map.filter(null != _._2).foreach(it =>
      props.put(it._1,
        it._2 match {
          case null => null
          case ref: AnyRef => ref
        }
      ))
    props
  }
  
  def jobProperties: Map[String, Any] = config.get("jobProp").getOrElse(Map.empty)
      
  def toSQLType(dataType: String): Int = {
        dataType.toUpperCase() match {
            case "VARCHAR" => Types.VARCHAR
            case "CHAR" => Types.VARCHAR
            case "STRING" => Types.VARCHAR
            case "BOOLEAN" => Types.BOOLEAN
            case "FLOAT" => Types.DECIMAL
            case "DOUBLE" => Types.DECIMAL
            case "DECIMAL" => Types.DECIMAL
            case "SHORT" => Types.SMALLINT
            case "INTEGER" => Types.INTEGER
            case "INT" => Types.INTEGER
            case "LONG" => Types.BIGINT
            case "BIGINT" => Types.BIGINT
            case "DATETIME" => Types.TIMESTAMP
            case "DATE"  => Types.TIMESTAMP
            case "TIME"  => Types.TIMESTAMP
            case "XML"=> Types.VARCHAR
            case _ => Types.VARCHAR
        }
    }
}

