package com.rnctech.common

import java.io.InputStream
import java.text.{ FieldPosition, SimpleDateFormat }
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.util
import java.util.Date
import scala.collection.JavaConverters._
import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.node.ArrayNode
import com.rnctech.common.HttpRestClient.{ NoSSL, SSLSupport, SSLWithDefaultRandom, SSLWithStrongRandom }
import com.rnctech.common.utils.ScalaObjectMapper
import com.rnctech.common.utils.UpdateProgressMessage
import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration
import scala.concurrent.forkjoin.ForkJoinPool
import scala.concurrent.{ Await, ExecutionContext, Future }
import scala.util.{ Failure, Success, Try }

/**
 * Implementations for communication with configuration service
 * Created by Zilin on 1/7/2021.
 */

class NRConfig(val baseUrl: String, val user: String = null, val password: String = null,
               override val requireSSL: SSLSupport = SSLWithStrongRandom, val useConfig: Boolean = true)

  extends HttpRestClient with NRConfigAPI {

  def this(baseUrl: String, user: String, password: String, requireSSL: SSLSupport) =
    this(baseUrl, user, password, requireSSL, true)

  def this(baseUrl: String, user: String, password: String) =
    this(baseUrl, s"$user@rnctech.com", s"$password", NoSSL, false)

  private final val CONFIG_CATEGORY_QUERY = ""

  private final val CONFIG_CATEGORY_QUERY_APPEND = if (CONFIG_CATEGORY_QUERY.isEmpty) "" else s"&$CONFIG_CATEGORY_QUERY"

  override def getAllProperties(): java.util.Map[String, java.util.Set[String]] =
    getObject[java.util.Map[String, java.util.Set[String]]](s"setting/all")

  override def getProperty(key: String): String =
    getObject[String](s"setting/$key")

  override def getSourceProperties(source: String): java.util.Map[String, String] =
    getObject[java.util.Map[String, String]](s"setting/$source/all")

  override def updateProperties(props: java.util.Map[String, String]): Unit =
    putObjectNoResponse(s"setting/props", props)

  override def updateProperties(name: String, value: String): Unit =
    putObjectNoResponse(s"setting/prop/$name", value)

  override def getStatus(id: String): String =
    getObject[String](s"job/$id")

  override def setStatus(id: String, status: String): Unit =
    putObjectNoResponse(s"job/$id/status", status)

  override def getModel(ctx: String, modelname: String): Array[Byte] =
    getObject[Array[Byte]](s"model/$ctx/$modelname")

  override def uploadModel(ctx: String, modelname: String, data: InputStream): Unit =
    putObjectNoResponse(s"model/$ctx/$modelname", data)

  private object ETLTimestampFormat extends SimpleDateFormat("yyyy-MM-dd HH:mm:ss") {
    override def format(date: Date, toAppendTo: StringBuffer, pos: FieldPosition): StringBuffer = this.synchronized(
      super.format(date, toAppendTo, pos))
  }

  private final val DEFAULT_SALT: String = "*F5+D)$?[?;{7,IV"

  private final val MAX_COMMENT_LENGTH = 240

  def getPrimaryKeysAsJava(instance: String, entityName: String): util.Map[String, String] = {
    val primKeys = getJson(s"sources/sourcesysteminstances/$instance/entities/$entityName/keys").asInstanceOf[ArrayNode]
      .elements().asScala.map(it => (it.at("/keyName").asText(), it.at("/sourceEntityAttributes").asInstanceOf[ArrayNode].elements().asScala.map(it2 => (it2.at("/name").asText(), it2.at("/dataType").asText()))))
    val rawMap = scala.collection.mutable.Map[String, Iterator[(String, String)]]()
    primKeys foreach { case (key, value) => rawMap.put(key, value) }
    val map = scala.collection.mutable.Map[String, String]()
    for ((key, value) <- rawMap) {
      for ((k, v) <- value) map.put(k, v)
    }
    map
  }.asJava

  private def getCategoryEntities(category: String, forExtractionOnly: Boolean = false): Map[String, Set[String]] = {
    val entities = getJson(if (forExtractionOnly) s"sources/categories/$category/entities/?extract=true" else s"sources/categories/$category/entities/").asInstanceOf[ArrayNode]
      .elements().asScala.map(it => (it.at("/name").asText(), it.at("/instance/name").asText()))
    entities.toSeq
      .groupBy(_._2)
      .map(it => (it._1, it._2.map(_._1).toSet))
  }

  private implicit val executionContext = ExecutionContext.fromExecutorService(new ForkJoinPool(Math.max(Runtime.getRuntime.availableProcessors(), 4)))

  def getEntities(categories: Seq[String], forExtractionOnly: Boolean = false) = {
    Try {
      categories.flatMap(cat =>
        ScalaObjectMapper.readValue(cat, new TypeReference[Map[String, Set[String]]] {}).asInstanceOf[Map[String, Set[String]]]).groupBy(_._1).map(it => it._1 -> it._2.tail.foldLeft(it._2.head._2)((a, v) => a ++ v._2)).toMap
    }.getOrElse {
      val tasks = categories.map(category => Future(getCategoryEntities(category, forExtractionOnly)))
      Await.result(Future.sequence(tasks), Duration.Inf)
        .flatMap(entities => entities.toSeq)
        .groupBy(_._1)
        .map(entities => (entities._1, entities._2.flatMap(_._2).toSet))
    }
  }

  def getEntityPropertiesAndConfig(instanceName: String, entityName: String): List[((String, String, String), String)] =
    Await.result(Future.sequence(
      Seq(getEntitySourceConfig _, getEntityProperties _)
        .map(f => Future(f(instanceName, entityName)))), Duration.Inf)
      .toList.zipWithIndex.flatMap(items => items._1.toSeq.map(it => it._1 -> (it._2, items._2)))
      .groupBy(_._1).map(it => it._1 -> it._2.map(_._2).sortBy(_._2).head._1)
      .toList

  def getSourceInstancePropertiesAndConfig(instanceName: String): List[((String, String), String)] =
    Await.result(Future.sequence(
      Seq(getSourceInstanceConfig _, getSourceInstanceProperties _)
        .map(f => Future(Try { f(instanceName) }))), Duration.Inf)
      .toList.filter(_.isSuccess).map(_.get).zipWithIndex.flatMap(items => items._1.toSeq.map(it => it._1 -> (it._2, items._2)))
      .groupBy(_._1).map(it => it._1 -> it._2.map(_._2).sortBy(_._2).head._1)
      .toList

  def getEntityProperties(instanceName: String, entityName: String) =
    getObject[Map[String, Any]](s"sources/sourcesysteminstances/$instanceName/entities/$entityName/properties?ignoreException=true")
      .map(it => ((instanceName, entityName, it._1), it._2.toString)).toList

  def getEntitiesProperties(instanceName: String, entities: Set[String]): Map[String, Map[String, String]] =
    postObject[Map[String, Map[String, String]]](s"sources/sourcesysteminstances/$instanceName/entities/properties", entities)

  def getEntitiesConfigurations(instanceName: String, entities: Set[String]): Map[String, Map[String, String]] =
    if (useConfig)
      postObject[Map[String, Map[String, String]]](s"configurations/sourceinstances/$instanceName/entities?$CONFIG_CATEGORY_QUERY", entities)
    else
      Map()

  def getEntitySourceConfig(instanceName: String, entityName: String) =
    if (useConfig)
      getObject[Map[String, Any]](s"configurations/sourceinstances/$instanceName/entities/$entityName?$CONFIG_CATEGORY_QUERY")
        .map(it => ((instanceName, entityName, it._1), it._2.toString)).toList
    else
      Map()

  def getSourceInstanceConfig(instanceName: String) =
    if (useConfig) {
      val cfg = getObject[Map[String, Any]](s"configurations/sourceinstances?name=$instanceName$CONFIG_CATEGORY_QUERY_APPEND")
      cfg.map(it => ((instanceName, it._1), it._2.toString)).toList
    } else
      Map()

  def getSourceInstanceProperties(instanceName: String) =
    getObject[Map[String, String]](s"sources/sourcesysteminstances/$instanceName/properties")
      .map(it => ((instanceName, it._1), it._2)).toList

  def updateEntityProperties(instanceName: String, properties: Map[String, Map[String, String]]): Unit = {
    val props = properties.map(entity => entity._1 ->
      entity._2.map(property => property._1 -> Map("value" -> property._2)).toMap)
    putObjectNoResponse(s"sources/ssi/$instanceName/entityproperties", Map("sourceEntities" -> props))
  }

  def updateEntityProperties(sourceInstance: String, entity: String, properties: Map[String, String]): Unit =
    updateEntityProperties(sourceInstance, Map(entity -> properties))

  def updateEntityProperty(sourceInstance: String, entity: String, name: String, value: String): Unit =
    updateEntityProperties(sourceInstance, Map(entity -> Map(name -> value)))

  def updateSourceInstanceProperties(instanceName: String, properties: Map[String, String]) =
    postObjectNoResponse(s"sources/sourcesysteminstances/$instanceName/properties", Map("property" -> properties))

  def getSourceSystemsS: Stream[(String, String)] = {
    val s = getObject[Set[String]]("sources")
    s.toStream.map(source => getObject[Set[Map[String, Any]]](s"sources/$source/sourcesysteminstances")
      .map(instance => (source, instance("name").asInstanceOf[String])))
      .flatMap(it => it.toStream)
  }

  def getSourceSystems: Stream[(String, String)] = {
    getObject[Map[String, String]]("sources/instancesNamemap").toStream
    /* Await.result(Future(getObject[Set[String]]("sources"))
      .flatMap((ss: Set[String]) =>
        Future.sequence(
          ss.toStream.map(source => Future(getObject[Set[Map[String, Any]]](s"sources/$source/sourcesysteminstances"))
            .map(instances => (source, instances.map(_ ("name").asInstanceOf[String]))))))
      , Duration.Inf).flatMap(it => it._2.map((_, it._1)))*/
  }

  protected def sendJobStatusUpdate(jobData: JobData) {
    debug(jobData.jobStatus)
    putObjectNoResponse(s"jobs/status", JobStatus(jobData.jobStatus, jobData.jobStatusComment))
    debug(jobData.jobStatus)
  }

  case class JobStatus(val status: String, statusComment: String)

  implicit class JobDataExt(jobData: JobData) {
    def updateProperties(properties: Map[String, String]) = {
      jobData.jobProperties ++= properties
      putObjectNoResponse(s"jobs/properties", jobData.jobProperties)
    }

    def updateFailures(instance: String, entities: Set[String]): Unit =
      jobData.failures =
        if (null == jobData.failures)
          Seq(instance -> entities).toMap
        else
          jobData.failures.filter(_._1 != instance) + jobData.failures.find(_._1 == instance).map(entry => instance -> (entry._2 ++ entities)).getOrElse(instance -> entities)

    def updateStatus(status: NRDataSink.STATUS, message: String, withParent: Boolean = false): Unit = {
      jobData.jobStatusComment = message.substring(0, Math.min(message.length, MAX_COMMENT_LENGTH))
      status match {
        case NRDataSink.STATUS.INITIALIZED
          | NRDataSink.STATUS.STARTED | NRDataSink.STATUS.PROGRESSING =>
          jobData.jobStatus = status.name
          if (null == jobData.jobStartTimestamp) {
            jobData.jobStartTimestamp = new Date()
            sendJobUpdate(jobData, true)
          } else {
            sendJobStatusUpdate(jobData)
          }
        case NRDataSink.STATUS.COMPLETED | NRDataSink.STATUS.FAILED
          | NRDataSink.STATUS.CANCELLED | NRDataSink.STATUS.COMPLETED_WITH_ERRORS
          | NRDataSink.STATUS.COMPLETED_WITH_WARNINGS =>
          jobData.jobStatus = status.name
          jobData.jobEndTimestamp = new Date()
          if (null != jobData.failures && jobData.failures.nonEmpty) {
            val failed: String = ScalaObjectMapper.writeValueAsString(jobData.failures)
            jobData.jobProperties += "failed_entities" -> failed
          }
          sendJobUpdate(jobData, withParent)
      }
    }

    def sendJobUpdate(jobData: JobData, withParent: Boolean): Unit =
      print(jobData.toString)

    def updateStatus(status: NRDataSink.STATUS, progress: Double): Unit =
      updateStatus(status, f"Executed $progress%0.2f%%")

    def getEntitiesRaw() = {
      Try {
        val tr = new TypeReference[Map[String, Set[String]]] {}
        val categories: Map[String, Set[String]] = ScalaObjectMapper.readValue(jobData.entityCategory, tr)
        categories
      }.getOrElse(getEntities(jobData.getCategories, true))
    }
    def getCategories: List[String] = jobData.entityCategory match {
      case null => List()
      case _    => jobData.entityCategory.split(",").map(_.trim).toList
    }
  }

  def getTenantProperty(key: String): String =
    Option(getAsString(s"tenantproperties/$key")).filter(!_.isEmpty)
      .getOrElse(
        Option(getJson(s"configure/properties/list.json?pType=TenantSystem&qs=$key"))
          .map(_.at("/properties").asInstanceOf[ArrayNode])
          .map(_.elements().asScala.toList.filter(_.at("/name").asText() == key))
          .filter(1 == _.size)
          .map(_.head.at("/tenantPropertyValue/value").asText)
          .orNull)

  override def close(): Unit = {
    //this.httpclient.close()
    executionContext.shutdown()
    super.close()
  }

  def updateStatus(message: UpdateProgressMessage): Unit = {
    debug("update status")
    val timestamp = "timestamp" -> new java.util.Date()
    val phase = "phase" -> "extraction"
    val statuses = message.entities.asScala.flatMap { entities =>
      val status = "status" -> (message.status match {
        case NRDataSink.STATUS.COMPLETED | NRDataSink.STATUS.COMPLETED_WITH_WARNINGS => "SUCCESS"
        case NRDataSink.STATUS.CANCELLED | NRDataSink.STATUS.COMPLETED_WITH_ERRORS |
          NRDataSink.STATUS.FAILED => "FAILURE"
        case _ => "INPROGRESS"
      })
      entities._2.asScala.map(name =>
        Map("entityName" -> s"${entities._1}:$name", status, timestamp, phase))
    }
    putObjectNoResponse(s"jobentitystatus/job", statuses.toList)
  }

  def order(f: (String, Set[String]) => Map[String, Map[String, String]], i: Int) =
    (inst: String, entities: Set[String]) => f(inst, entities).map(it => it._1 -> it._2.map(it1 => (it1._1 -> 1) -> it1._2))

  def getEntitiesPropertiesAsJava(instance: String, entities: util.Set[String]): util.Map[String, util.Map[String, String]] = {
    val results = Future.sequence(
      Seq(order(getEntitiesConfigurations, 0)(_, _), order(getEntitiesProperties, 1)(_, _))
        .map(f => Future(f(instance, entities.asScala.toSet))))
    Await.ready(results, Duration.Inf).value.get match {
      case Success(res) => res.toList.flatMap(items => items.toList).groupBy(_._1)
        .map(it => it._1 -> it._2.flatMap(_._2.toList).groupBy(_._1._1).map(it => it._1 -> it._2.sortBy(_._2).head._2))
        .map(it => it._1 -> it._2.asJava).asJava
      case Failure(th) => throw th
    }
  }

}

@JsonIgnoreProperties
case class JobData(var jobId: Long, var tenantName: String,
                   var sourceSystemName: String, var sourceSystemInstanceName: String,
                   var entityCategory: String, var jobName: String,
                   var jobStatus: String, var jobStatusComment: String,
                   var jobExecutionCount: Int, var parentJobId: java.lang.Long,
                   var externalParentJobId: java.lang.Long, var jobType: String,
                   var loadType: String, var jobProperties: Map[String, String],
                   var createdBy: String, var updatedBy: String,
                   var guid: String, var jobRegistrationTimestamp: Date,
                   var jobStartTimestamp: Date, var jobEndTimestamp: Date,
                   var created: Date, var updated: Date,
                   @transient var failures:             Map[String, Set[String]],
                   @JsonIgnoreProperties childJobs:     List[JobData],
                   @JsonIgnoreProperties attachedFiles: List[Map[Long, String]])


