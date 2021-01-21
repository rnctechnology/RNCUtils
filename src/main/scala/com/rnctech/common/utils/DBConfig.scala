package com.rnctech.common.utils

import java.text.SimpleDateFormat
import org.apache.commons.lang3.StringUtils
import org.apache.commons.dbcp2.BasicDataSource
import javax.sql.DataSource

/**
* Created by Alan Chen on 3/20/2020.
*/
  
class DBConfig(val ssiname: String, val url: String, val username: String, val password: String) {  
  
  def dbProduct: DBProduct =  {
    if(StringUtils.contains(url, "mysql"))
      mysql
    else if(StringUtils.contains(url, "oracle"))
      oracle
    else if(StringUtils.contains(url, "sqlserver"))
      sqlserver
    else if(StringUtils.contains(url, "informix"))
      informix
    else if(StringUtils.contains(url, "redshift"))
      redshift
    else
      hsqldb
  }
  
  lazy val dataSource: DataSource = {
    val ds = new BasicDataSource
    ds.setDriverClassName(dbProduct.driverClass)
    ds.setUrl(url)
    ds.setUsername(username)
    ds.setPassword(password)
    ds.setDefaultQueryTimeout(defaultQueryTimeout)
    ds.setMaxTotal(maxTotal)
    ds.setMaxIdle(maxIdle)
    ds.setInitialSize(initialSize)
    ds
  }  
  
  val maxIdle: Int = 4
  val initialSize: Int =4
  val maxTotal = 16
  var defaultQueryTimeout: Integer = null  // default to driver setting
  var dbservertimezone = "UTC"
 
  override def toString: String =
    s"""
       |username: $username
       |password: $password
       |driverClassName: ${dbProduct.driverClass}
       |url: $url
  """.stripMargin  
}

sealed abstract class DBProduct( val name: String,
                               val driverClass: String,
                               val localStorage: Boolean)
case object oracle extends DBProduct("oracle", "oracle.jdbc.driver.OracleDriver", true)
case object mysql extends DBProduct("mysql", "com.mysql.jdbc.Driver", true)
case object sqlserver extends DBProduct("sqlserver", "net.sourceforge.jtds.jdbc.Driver", true)
case object informix extends DBProduct("informix", "com.informix.jdbc.IfxDriver", true)
//jdbc:redshift://localhost:5439/dbname
case object redshift extends DBProduct("redshift", "com.amazon.redshift.jdbc4.Driver", false)
//jdbc:hsqldb:mem:adb
case object hsqldb extends DBProduct("hsqldb", "org.hsqldb.jdbcDriver", false)