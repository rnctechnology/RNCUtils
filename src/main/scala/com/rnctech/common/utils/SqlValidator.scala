package com.rnctech.common.utils

import net.sf.jsqlparser.JSQLParserException
import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.statement.{Statement, StatementVisitor}
import net.sf.jsqlparser.util.{SelectUtils,TablesNamesFinder}

/**
* Created by Alan on 3/30/2018.
*/

object SqlValidator extends Logging {

  def validateSql(sql: String, types: Seq[String]): Unit = {
    try {
      CCJSqlParserUtil.parse(sql)
    } catch {
      case e: JSQLParserException => error(sql, e)
    }
  }

  @throws[Exception]
  def validateSql(entity: String, sql: String): Unit = {
      val stat: Statement =  CCJSqlParserUtil.parse(sql)
      val tfinder = new TablesNamesFinder()
      val yn: Boolean = tfinder.getTableList(stat).contains(entity)
      if(!yn) throw new Exception(s"No $entity found in $sql")
      // do more ??    
  }
}
