package com.rnctech.common

import java.net.URI
import java.util.concurrent.{Executors, TimeUnit}
import com.rnctech.common.utils.Logging
import com.rnctech.common.HttpRestClient.{SSLSupport, SSLWithDefaultRandom}
import org.apache.http.{HttpRequest, HttpRequestInterceptor}
import org.apache.http.client.methods._
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.protocol.HttpContext
import scala.collection.JavaConverters._
import scala.ref.SoftReference

class SimpleRestClient(override protected val baseUrl: String, private final val headers : java.util.Map[String, String],
  override protected val user: String = "none",
  override protected val password: String = "none",
  override protected val reqAuthenticate: Boolean = false,
  override protected val requireSSL: SSLSupport = SSLWithDefaultRandom,
  override protected val proxyHostName: String = System.getProperty(Constants.TENANT_PROXY_HOST),
  override protected val proxyPort: Int = Option(System.getProperty(Constants.TENANT_PROXY_PORT)).map(_.toInt).getOrElse(-1),
  override protected val proxyUser: String = System.getProperty(Constants.TENANT_PROXY_USER),
  override protected val proxyPassword: String = System.getProperty(Constants.TENANT_PROXY_PWD)) extends HttpRestClient with Logging{

  def this(baseUrl: String, headers : java.util.Map[String, String]) {
    this(baseUrl, headers, "none", "none", false,
  SSLWithDefaultRandom, System.getProperty(Constants.TENANT_PROXY_HOST),
  Option(System.getProperty(Constants.TENANT_PROXY_PORT)).map(_.toInt).getOrElse(-1),
  System.getProperty(Constants.TENANT_PROXY_USER),
  System.getProperty(Constants.TENANT_PROXY_PWD))
  }

  def this(baseUrl: String) = {
    this(baseUrl, java.util.Collections.emptyMap[String, String]())
  }

  private implicit def toMap(map : java.util.Map[String, String]) : Map[String, String] = map.asScala.toMap
  override implicit protected def toHttpGet(uri: URI): HttpGet = super.toHttpGet(uri).withHeaderParams(headers)
  override implicit protected def toHttpPut(uri: URI): HttpPut = super.toHttpPut(uri).withHeaderParams(headers)
  override implicit protected def toHttpPost(uri: URI): HttpPost = super.toHttpPost(uri).withHeaderParams(headers)

  override protected def beforeCreate(clientBuilder: HttpClientBuilder): Unit = {
    clientBuilder.addInterceptorLast(new HttpRequestInterceptor {
      override def process(request: HttpRequest, context: HttpContext): Unit = {
        request match {
          case abortable : AbstractExecutionAwareRequest =>
            val softRef = new SoftReference(abortable)
            SimpleRestClient.scheduledExecutrors.schedule(new Runnable {
              override def run(): Unit = {
                softRef.get.filter(!_.isAborted).foreach{
                  _.abort()}
              }
            }, 1, TimeUnit.MINUTES)
          case wrapped : HttpRequestWrapper =>
            process(wrapped.getOriginal, context)
          case _ =>
        }
      }
    })
  }
}

object SimpleRestClient {
  val scheduledExecutrors = Executors.newScheduledThreadPool(10)
}
