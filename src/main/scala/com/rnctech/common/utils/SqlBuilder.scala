package com.rnctech.common.utils

import scala.reflect.macros.blackbox.Context
import net.sf.jsqlparser.JSQLParserException
import net.sf.jsqlparser.parser.CCJSqlParserUtil
import com.rnctech.common.Constants
import java.text.{SimpleDateFormat, DateFormat}
import java.lang.{ StringBuilder => JLSBuilder }

/**
* Created by Alan on 4/16/2018.
*/

object SqlBuilder extends DateTimeUtil {
  
  final val oraTimeFormat = "'yyyy-mm-dd hh24:mi:ss'"
  final val queryAll = "SELECT * FROM"
  final val queryAllWithSTO = "SELECT /*+ MAX_EXECUTION_TIME(120000) */ * FROM"
  final val queryLTO = "SET LOCK_TIMEOUT "
  
  def buildSqlForCount(entity: String, props: EntityProp, instantprop: Map[String, Any]): String = {
    var sql = "SELECT count(*) FROM (" + buildSql(entity, props, instantprop) +")" 
    val dbtype = instantprop.get("DBType").get
    var alias =  dbtype match {
      case "mysql" => " TTCC"
      case _ => ""
    }
    sql + alias
  }
  
  //select <tableName>.*, tm_actuals.creation_date, tm_actuals.last_update_date from <tableName> join tm_actuals on <tableName>.actuals_id = tm_actuals.actuals_id where tm_actuals.last_update_date 
  //>= to_date(<startTimeStampForQuery>,'yyyy-mm-dd hh24:mi:ss')
  
  def buildSql(entity: String, props: EntityProp, instantprop: Map[String, Any]): String = {
    if(null == props.query || props.query.isEmpty()){ 
      props.letcolumn match {
        case null => s"$queryAll $entity"  
        case "" => s"$queryAll $entity"
        case column => {
          val let = props.let
          val dbtype = instantprop.get("DBType").get
          dbtype match {
            case "oracle" => s"$queryAll $entity WHERE $column >= to_date('$let', $oraTimeFormat)"
            case _ =>  s"$queryAll $entity WHERE $column >= '$let'"
          }
        }     
      }
    }else {
      val let = props.let
      val letexpr = instantprop.get("DBType").get match {
        case "oracle" => s" >= to_date('$let', $oraTimeFormat)"
        case _ =>  s" >= '$let'"
      }
      props.query.replaceAll("<tableName>", entity) + letexpr
    }
  }
  
  def buildSqlFirstRow(entity: String, props: EntityProp, instantprop: Map[String, Any]): String = {    
    var sql = sample(instantprop, buildSql(entity, props, instantprop))
    sql
  }
  
  def buildSqlForSample(entity: String, props: EntityProp, instantprop: Map[String, Any], count: Int = 10): String = {
    sample(instantprop, buildSql(entity, props, instantprop), count.toString)    
  }
  
  def sample(instantprop: Map[String, Any], sql: String, limit: String = "1"): String = {
    val dbtype = instantprop.get("DBType").get
    dbtype match {
            case "oracle" => {
              if(sql.toLowerCase().contains("where")){
                  s"$sql and rownum <= $limit"
              }else{
                  s"$sql where rownum <= $limit"
              }
            }
            case "sqlserver" =>  sql.replaceAll("(?i)select * ", "SELECT TOP $limit ")
            case _ => s"$sql limit $limit"
    }    
  }
  
  def validateQuery(instantprop: Map[String, Any], entity: String, sql: String, count: Int = 10): String = {
    val sqlquery = sql.replaceAll("<tableName>", entity)
    SqlValidator.validateSql(entity, sqlquery)
    sample(instantprop, sqlquery, count.toString())
  }
  
}
