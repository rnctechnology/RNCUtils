package com.rnctech.common

import java.io.{Closeable, File, IOException, InputStream}
import java.net.URI
import java.nio.charset.Charset
import java.security.{SecureRandom, cert}

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.rnctech.common.utils.Logging
import com.rnctech.common.utils.ScalaObjectMapper
import javax.net.ssl.{SSLContext, X509TrustManager}
import org.apache.http._
import org.apache.http.auth.{AuthSchemeProvider, AuthScope, AuthenticationException, UsernamePasswordCredentials}
import org.apache.http.client.RedirectStrategy
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.entity.UrlEncodedFormEntity
import org.apache.http.client.methods._
import org.apache.http.client.protocol.HttpClientContext
import org.apache.http.client.utils.URIBuilder
import org.apache.http.config.{Lookup, RegistryBuilder}
import org.apache.http.conn.socket.{ConnectionSocketFactory, PlainConnectionSocketFactory}
import org.apache.http.conn.ssl.{NoopHostnameVerifier, SSLConnectionSocketFactory}
import org.apache.http.cookie.Cookie
import org.apache.http.entity.mime.{HttpMultipartMode, MultipartEntityBuilder}
import org.apache.http.entity.{ContentType, StringEntity}
import org.apache.http.impl.auth.BasicSchemeFactory
import org.apache.http.impl.client.{HttpClientBuilder, _}
import org.apache.http.impl.conn.{ConnectionShutdownException, PoolingHttpClientConnectionManager}
import org.apache.http.impl.cookie.BasicClientCookie
import org.apache.http.message.BasicNameValuePair
import org.apache.http.protocol.HttpContext
import org.apache.http.util.EntityUtils
import HttpRestClient._
import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.io.Source
import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.util.{Random, Try}

trait HttpRestClient extends Logging with Closeable {

  protected val baseUrl : String
  protected val user : String
  protected val password : String
  protected val reqAuthenticate: Boolean = true
  protected val requireSSL : SSLSupport = SSLWithStrongRandom
  protected val proxyHostName : String = null
  protected val proxyPort : Int = 0
  protected val proxyUser : String = null
  protected val proxyPassword : String = null
  protected val USE_DEFAULT_COOKIE_SPEC = true

  private implicit def toNamedValuePair(value : (String, String)): BasicNameValuePair =
    new BasicNameValuePair(value._1, value._2)

  def onBody(request : HttpEntityEnclosingRequest with HttpUriRequest) = request
  
  implicit class HttpEntityRequestExt[REQ <: HttpEntityEnclosingRequest with HttpUriRequest](val request : REQ) {
    def withContentType(contentType : String = TYPE_JSON) : REQ = {request.addHeader("Content-Type", contentType); request}
    def withEntity[T](value : T) : REQ = {
      request.setEntity(new StringEntity(value match {
        case str : String => str
        case _ => Try(ScalaObjectMapper.writeValueAsString(value)).getOrElse(new ObjectMapper().writeValueAsString(value))}, Charset.forName("UTF-8")))
      request
    }
    def withEntity(value : String) : REQ = {request.setEntity(new StringEntity(value, Charset.forName("UTF-8")))
      request
    }
    def withObject[T](value : T) : REQ = withContentType(TYPE_JSON).withEntity(value)
    def withObject(value : String) : REQ = withContentType(TYPE_JSON).withEntity(value)
    def withEntityObject[T <: HttpEntity](value : T) : REQ = {request.setEntity(value); request}
  }

  implicit class HttpRequestExt[REQ <: HttpUriRequest](val request : REQ) {
    def withAcceptType(acceptType : String = TYPE_JSON) : REQ = {request.addHeader("Accept", acceptType); request}
    def withHeaderParams(params : Map[String, String]): REQ = { Option(params).foreach(_.foreach{
        case (key: String, value:String) =>
          request.setHeader(key, value)
        case _ =>
        }
      ) 
      request
    }
  }

  private def retryIfDisconnected[U](block : => U): U = {
    val exceptions = Seq(classOf[ConnectionClosedException], classOf[ConnectionShutdownException],classOf[java.net.ConnectException],
      classOf[java.net.UnknownHostException]
    )
    @tailrec def digUpException(th : Throwable, classes : Class[_]*): Throwable = {
      th.getCause match {
        case null => th
        case cause if cause == th => th
        case cause if classes.exists(_.isAssignableFrom(cause.getClass)) =>
          cause
        case cause => digUpException(cause, classes : _*)
      }
    }
    try {
      block
    } catch {
      case th : Throwable if exceptions.exists(_.isAssignableFrom(digUpException(th, exceptions : _*).getClass)) =>
        debug(th.getMessage)
        Thread.sleep(Random.nextInt(1000))
        block
      case th : Throwable => throw th
    }
  }

  protected def beforeCreate(clientBuilder : HttpClientBuilder): Unit = {
  }
  
  def setCookie(name : String, value : String): Unit = {
    context.getCookieStore.addCookie(new BasicClientCookie(name, value))
  }

  def setCookie(cookie : Cookie): Unit = {
    context.getCookieStore.addCookie(cookie)
  }

  def getCookies: Seq[Cookie] = {
    context.getCookieStore.getCookies.asScala
  }

  protected lazy implicit val httpclient: CloseableHttpClient = {
    val builder = RegistryBuilder.create[ConnectionSocketFactory].register("http", new PlainConnectionSocketFactory)
    requireSSL match {
      case NoSSL =>
      case _     =>
        val sslContext = SSLContext.getInstance("TLS")
        //Accept all certificates. The assumption is this client accesses servers within VPC, so it is safe
        sslContext.init(null, Array(new X509TrustManager {
          override def checkServerTrusted(x509Certificates: Array[cert.X509Certificate], s: String): Unit = {}
          override def checkClientTrusted(x509Certificates: Array[cert.X509Certificate], s: String): Unit = {}
          override def getAcceptedIssuers: Array[cert.X509Certificate] = null
        }), requireSSL match {
            case SSLWithStrongRandom => SecureRandom.getInstanceStrong
            case _                   => new SecureRandom
          }
        )
        builder.register("https", new SSLConnectionSocketFactory (sslContext, NoopHostnameVerifier.INSTANCE))
    }
    
    def setupProxy(builder : HttpClientBuilder): HttpClientBuilder = {
      val credentialsProvider = new BasicCredentialsProvider
      Option(proxyHostName).foreach{ _ =>
        val proxyHost = new HttpHost(proxyHostName, proxyPort)
        if(null != proxyUser && !proxyUser.trim().isEmpty && !proxyUser.equalsIgnoreCase("none") && !proxyUser.equalsIgnoreCase("null")) {
          credentialsProvider.setCredentials(new AuthScope(proxyHostName, proxyPort), new UsernamePasswordCredentials(proxyUser, proxyPassword))
          debug(s"auth user $proxyUser with proxy ${proxyHost.toURI} .")
        }else{
          debug(s"using proxy ${proxyHost.toURI}.")
        }
      }
      if(null!= user && null != password)
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(user, password))
      builder.setDefaultCredentialsProvider(credentialsProvider)
      builder
    }
    
    val connectionManager =  new PoolingHttpClientConnectionManager(builder.build())
    val clientBuilder: HttpClientBuilder = setupProxy(HttpClients.custom.setConnectionManager(connectionManager)
      .setRedirectStrategy(getRedirectStrategy).setRetryHandler(new StandardHttpRequestRetryHandler)
    )

    beforeCreate(clientBuilder)
    clientBuilder.build

  }

  protected def getRedirectStrategy: RedirectStrategy = {
        new DefaultRedirectStrategy {
          override def getRedirect(request: HttpRequest, response: HttpResponse, context: HttpContext): HttpUriRequest =
            super.getRedirect(request, response, context)
          override def isRedirected(request: HttpRequest, response: HttpResponse, context: HttpContext): Boolean =
            (Option(response.getFirstHeader("Location")).getOrElse("") != "") && super.isRedirected(request, response, context)

        }
  }

  implicit def toLookup[T](f : String => T) : Lookup[T] = new Lookup[T] {override def lookup(s: String): T = f(s)}

  private val authRegistry : Lookup[AuthSchemeProvider] = (_ :  String) => new BasicSchemeFactory

  protected implicit val context: HttpClientContext = if(USE_DEFAULT_COOKIE_SPEC) HttpClientContext.create else {
    val ctx = HttpClientContext.create
    val reqConfigBuilder = RequestConfig.custom()
    reqConfigBuilder.setCookieSpec("standard")
    ctx.setRequestConfig(reqConfigBuilder.build())
    ctx
  }

  //context.setCredentialsProvider(credentialsProvider)
  context.setAuthSchemeRegistry(authRegistry)
  context.setAuthCache(new BasicAuthCache)

  private[this] lazy val authFormParams : Seq[BasicNameValuePair] = Seq(("password", password), ("username", user))

  protected def authenticate(): Unit = {
    val httpPost = toHttpPost(getURI("login")).withEntityObject(new UrlEncodedFormEntity(authFormParams.asJava, Consts.UTF_8))
    TRY(execute(httpPost))(response=> if(response.getStatusLine.getStatusCode >= HttpStatus.SC_BAD_REQUEST) {
        throw new AuthenticationException(response.getStatusLine.getReasonPhrase)
      }
    )
  }

  if(null != user && null != password && reqAuthenticate) authenticate()

  def TRY[T <: Closeable, R](create : => T)(f : T => R): R = {
    val closeable = create
    try {f(closeable)} finally {closeable.close()}
  }

  private implicit def toJson(response : HttpResponse): JsonNode = ScalaObjectMapper.readTree(
    response.getEntity.getContent
  )

  final def executeRaw[T <: HttpUriRequest](request : T) (implicit httpclient : CloseableHttpClient, httpcontext: HttpClientContext): CloseableHttpResponse =
    httpclient.execute(request, httpcontext)

  def execute[T <: HttpUriRequest](request : T) (implicit httpclient : CloseableHttpClient, httpcontext: HttpClientContext): CloseableHttpResponse =
    httpclient.execute(request, httpcontext)

  def getJson[T <: HttpUriRequest](request : T) :  JsonNode =
    retryIfDisconnected(TRY(execute(request.withAcceptType(TYPE_JSON)))(response => check(response)))

  def getJson[T <: HttpUriRequest](request : T, headers : Map[String, String]):  JsonNode = {
    retryIfDisconnected(TRY(execute(request.withAcceptType(TYPE_JSON)))(response => check(response)))
  }

  def getJsonWithHeaders[T <: HttpUriRequest](request : T):  (JsonNode,List[Header]) =
    retryIfDisconnected(TRY(execute(request.withAcceptType(TYPE_JSON)))(withHeaders(response =>check(response))))

  def getString(path : String): String =
    retryIfDisconnected(TRY(execute(toHttpGet(getURI(path))))(response=>
      EntityUtils.toString(check(response).getEntity)))


  def getXmlString(path : String): String =
    performWithRetry(toHttpGet(getURI(path)), TYPE_XML)(response=>
      EntityUtils.toString(check(response).getEntity))

  def withHeaders[T](f: HttpResponse => T)(response : HttpResponse) : (T, List[Header]) = f(response) -> response.headerIterator().asScala.map(_.asInstanceOf[Header]).toList
  def getAsString(path : String): String =
    retryIfDisconnected(TRY(execute(toHttpGet(getURI(path))))(response=>
      Source.fromInputStream(check(response).getEntity.getContent, "UTF-8").mkString))
  def getAsStringWithHeaders(path : String): (String, List[Header]) =
    retryIfDisconnected(TRY(execute(toHttpGet(getURI(path))))(withHeaders(response=>
      Source.fromInputStream(check(response).getEntity.getContent, "UTF-8").mkString)))
  def getAsString(path : String, encoding: String): String =
    retryIfDisconnected(TRY(execute(toHttpGet(getURI(path))))(response=>
      EntityUtils.toString(check(response).getEntity, encoding)))

  def getAsString(path : String, query : Option[String]): String =
    retryIfDisconnected(TRY(execute(toHttpGet(getURI(path, query))))(response=>
      Source.fromInputStream(check(response).getEntity.getContent, "UTF-8").mkString))

  def getJson(path : String, req : AnyRef, usePost : Boolean = true ) : JsonNode =
    getJson((if(usePost) toHttpPost(getURI(path)) else toHttpPut(getURI(path))).withEntity(req))

  def getJson(path : String, headers : Map[String, String], body : AnyRef, contentType : String, usePost : Boolean) : JsonNode =
    getJson(withHeaders(if(usePost) toHttpPost(getURI(path)) else toHttpPut(getURI(path)), headers)
      .withContentType(contentType).withEntity(body))

  def getJson(path : String, headers : Map[String, String]) : JsonNode =
    getJson(toHttpGet(getURI(path)), headers)

  def getJson(path : String) : JsonNode = getJson(toHttpGet(getURI(path)))

  def getJsonWithHeaders(path : String) : (JsonNode, List[Header]) = getJsonWithHeaders(toHttpGet(getURI(path)))

  private def withHeaders[T <: HttpUriRequest](request : T, headers : Map[String, String]) : T = {
    def addHeaders(_request : T, _headers: List[(String, String)]) : T =
      _headers match {
        case Nil          => _request
        case head :: tail => _request.addHeader(head._1, head._2); addHeaders(_request, tail)
      }
    addHeaders(request, headers.toList)
  }

  protected def checkForLoginRedirect(response : CloseableHttpResponse): Unit =
    if (Option(response.getFirstHeader("Location")).getOrElse("") != "") {
      authenticate()
      throw new ConnectionClosedException("Authentication failed")
    }

  def check(response : HttpResponse): HttpResponse = {
    response.getStatusLine.getStatusCode match {
      case code if code < HttpStatus.SC_MULTIPLE_CHOICES => response
      case HttpStatus.SC_UNAUTHORIZED =>
        authenticate()
        throw new ConnectionClosedException("Authentication failed")
      case HttpStatus.SC_MOVED_TEMPORARILY =>
        authenticate()
        throw new ConnectionClosedException("Authentication failed")
      case _ => EntityUtils.toString(response.getEntity) match {
        case null  => throw new IOException(s"${response.getStatusLine.getStatusCode} ${response.getStatusLine.getReasonPhrase}")
        case msg if msg.isEmpty => throw new IOException(s"${response.getStatusLine.getStatusCode} ${response.getStatusLine.getReasonPhrase}")
        case msg => throw new IOException(s"${response.getStatusLine.getStatusCode} ${response.getStatusLine.getReasonPhrase}:\n$msg")
      }
    }
  }

  protected def getURI(path : String): URI = URI.create(s"$baseUrl/${new URI(null, path,null).getRawSchemeSpecificPart}").normalize()

  private def setProxy[T <: HttpRequestBase](req : T) = {
    Option(proxyHostName).foreach { _ =>
      val proxyHost = new HttpHost(proxyHostName, proxyPort)
      val proxyConfig = RequestConfig.custom()
        .setProxy(proxyHost)
        .setProxyPreferredAuthSchemes(List("Basic").asJava)
        .build()
      req.setConfig(proxyConfig)
    }
    req
  }
  implicit protected def toHttpGet(uri : URI) : HttpGet = setProxy(new HttpGet(uri))

  implicit protected def toHttpPut(uri : URI) : HttpPut = setProxy(new HttpPut(uri))

  implicit protected def toHttpPost(uri : URI) :
  HttpPost = setProxy(new HttpPost(uri))

  protected def getURI(path : String, query: Option[String]): URI =
    new URIBuilder(s"$baseUrl/${new URI(null, path,null).getRawSchemeSpecificPart}").setCustomQuery(query.orNull).build()

  def getObject[T : ClassTag](path : String, params: Map[String,String], query: Option[String] = Some(""))(implicit clazz : ClassTag[T]) : T =
    getObject(toHttpGet(getURI(path, query)).withHeaderParams(params))(clazz)

  def getObject[T : ClassTag](path : String)(implicit clazz : ClassTag[T]) : T =
    getObject(toHttpGet(getURI(path)))(clazz)

  protected def performWithRetry[T](request: HttpUriRequest, acceptType : String =TYPE_JSON)(responseHandler : CloseableHttpResponse => T) :T = {
    logRequest(request)
    retryIfDisconnected {
      TRY(execute(request.withAcceptType(acceptType)))(responseHandler)
    }
  }

  private def logRequest(request: HttpUriRequest) {
    trace(request.getMethod + request.getURI.toString)
  }

  protected def retrieveObject[T](request: HttpUriRequest, clazz : Class[T]) : T = performWithRetry(request){
    response =>ScalaObjectMapper.readValue(check(response).getEntity.getContent, clazz)
  }

  protected def send[T](request: HttpUriRequest) : CloseableHttpResponse = performWithRetry(request){ response =>check(response); response}

  def retrieveStream[T](request: HttpUriRequest) : InputStream = performWithRetry(request) { response =>
      check(response).getEntity.getContent
  }

  protected def getObject[T](request: HttpUriRequest)(clazz : ClassTag[T]) : T = performWithRetry(request) { response =>
      ScalaObjectMapper.readValue(check(response).getEntity.getContent, clazz.runtimeClass).asInstanceOf[T]
  }

  def putObject[T : ClassTag](path : String, value : AnyRef)(implicit clazz : ClassTag[T]) : T  =
    retrieveObject[T](toHttpPut(getURI(path)).withObject(value), clazz.runtimeClass.asInstanceOf[Class[T]])

  def putObject[T : ClassTag](path : String, value : String)(implicit clazz : ClassTag[T]) : T  =
    retrieveObject[T](toHttpPut(getURI(path)).withObject(value), clazz.runtimeClass.asInstanceOf[Class[T]])

  def postObject[T : ClassTag](path : String, headers: Map[String,String], value : AnyRef)(implicit clazz : ClassTag[T]) : T =
    retrieveObject[T](toHttpPost(getURI(path)).withObject(value).withHeaderParams(headers), clazz.runtimeClass.asInstanceOf[Class[T]])

  def postObject[T : ClassTag](path : String, headers: Map[String,String], value : String)(implicit clazz : ClassTag[T]) : T =
    retrieveObject[T](toHttpPost(getURI(path)).withObject(value).withHeaderParams(headers), clazz.runtimeClass.asInstanceOf[Class[T]])

  def postObject[T : ClassTag](path : String, value : AnyRef)(implicit clazz : ClassTag[T]) : T =
    retrieveObject[T](toHttpPost(getURI(path)).withObject(value), clazz.runtimeClass.asInstanceOf[Class[T]])

  def postObject[T : ClassTag](path : String, value : String)(implicit clazz : ClassTag[T]) : T =
    retrieveObject[T](toHttpPost(getURI(path)).withObject(value), clazz.runtimeClass.asInstanceOf[Class[T]])

  def postFormEncodedObject[T : ClassTag](path : String, value : Seq[(String, String)])(implicit clazz : ClassTag[T]) : T = {
    val formParams = value.map(toNamedValuePair).asJava
    retrieveObject[T](toHttpPost(getURI(path)).withEntityObject(new UrlEncodedFormEntity(formParams, Consts.UTF_8)), clazz.runtimeClass.asInstanceOf[Class[T]])
  }

  def getAsStringWithObject(path : String, value : AnyRef): String =
    retryIfDisconnected(TRY(execute(toHttpPost(getURI(path)).withObject(value)))(response=>
      Source.fromInputStream(check(response).getEntity.getContent, "UTF-8").mkString))

  def putObjectNoResponse(path : String, value : AnyRef) : Unit = send(toHttpPut(getURI(path)).withObject(value))

  def putObjectNoResponse(path : String, value : String) : Unit = send(toHttpPut(getURI(path)).withObject(value))

  def postObjectNoResponse(path : String, value : AnyRef) : Unit = send(toHttpPost(getURI(path)).withObject(value))

  def postObject(path : String, value : AnyRef, f : CloseableHttpResponse => Unit) : Unit = f(send(toHttpPost(getURI(path)).withObject(value)))

  def postObjectNoResponse(path : String, value : String) : Unit = send(toHttpPost(getURI(path)).withObject(value))

  def postObjectNoResponse(path : String, value : AnyRef, headers: java.util.Map[String,String]) : Unit =
    send(toHttpPost(getURI(path)).withObject(value).withHeaderParams(headers.asScala.toMap))

  def uploadFile(path: String, name : String, file : File) : Unit =
    uploadFile(path, builder=>builder.addBinaryBody(name, new java.io.FileInputStream(file),
      ContentType.APPLICATION_OCTET_STREAM, file.getName))

  def uploadFile(path: String, name : String, fileName : String, fileContent : Array[Byte]) : Unit =
    uploadFile(path, builder=>builder.addBinaryBody(name, fileContent, ContentType.APPLICATION_OCTET_STREAM, fileName))

  def uploadFile(path: String, name : String, fileName : String, fileContent : java.io.InputStream) : Unit =
    uploadFile(path, builder=>builder.addBinaryBody(name, fileContent, ContentType.APPLICATION_OCTET_STREAM, fileName))

  private def uploadFile(path: String, partProducer : MultipartEntityBuilder => Unit) : Unit = {
    val builder = MultipartEntityBuilder.create
    partProducer(builder)
    val uri = getURI(path)
    val request = toHttpPost(uri)
    logRequest(request)
    builder.setMode(HttpMultipartMode.STRICT)
    retryIfDisconnected(TRY(execute(request.withEntityObject(builder.build())))(response =>check(response)))
  }

  override def close() {
    httpclient.close()
  }
}

object HttpRestClient {
  class SSLSupport
  case object NoSSL extends SSLSupport
  case object SSLWithStrongRandom extends SSLSupport
  case object SSLWithDefaultRandom extends SSLSupport
  private final val TYPE_JSON = "application/json"
  private final val TYPE_XML = "text/xml"
}
