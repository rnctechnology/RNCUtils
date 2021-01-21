package com.rnctech.common

class SimpleHttpClient(val baseUrl : String, val user : String = null, val password : String = null,
    val jobId : Long = 0, override val requireSSL : HttpRestClient.SSLSupport = HttpRestClient.SSLWithDefaultRandom)
 extends HttpRestClient {
  def this(baseUrl : String, useStrongRandom : Boolean) = this(baseUrl, null, null, 0l,
    if(useStrongRandom)HttpRestClient.SSLWithStrongRandom else HttpRestClient.SSLWithDefaultRandom)
  def this(baseUrl : String) = this(baseUrl, false)
  override protected val reqAuthenticate: Boolean = false
}
