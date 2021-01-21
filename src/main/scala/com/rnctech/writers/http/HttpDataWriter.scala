package com.rnctech.writers.http

import java.net.URI
import java.time.{Duration, Instant}
import java.{lang, util}
import java.util.Date
import java.util.concurrent.{ExecutorService, Executors, TimeUnit}
import org.apache.http.client.methods.{HttpGet, HttpPost, HttpPut, HttpRequestBase}
import org.apache.avro.Schema
import scala.collection.JavaConverters._
import scala.collection.mutable.Map
import scala.concurrent.forkjoin.LinkedTransferQueue
import scala.util.Try
import org.json.{JSONArray, JSONObject}
import java.text.SimpleDateFormat
import java.util.{Calendar,TimeZone}
import com.rnctech.common.Constants.{PAYLOAD_SEQ,PAYLOAD_SEQ_ID,PAYLOAD_SEQ_CONTENT, TENANT_REQ_HEADER_PAYLOAD_SIZE, PAYLOAD_FROM}
import com.rnctech.common.Constants.{TENANT_PROXY_HOST,TENANT_PROXY_PORT,TENANT_PROXY_USER,TENANT_PROXY_PWD, HTTP_PAYLOAD_SIZE, HTTP_RETRY_COUNT, HTTP_RETRY_INTERVAL}
import scala.annotation.tailrec
import com.amazonaws.util.EC2MetadataUtils
import java.net.InetAddress
import com.rnctech.common.HttpRestClient
import com.rnctech.common.HttpRestClient._
import com.rnctech.common.utils._
import com.rnctech.common.utils.MetaConverters

/**
  * Created by Alan Chen on 2/10/2018.
  */
class HttpDataWriter(val baseUrl : String,  val user : String = null, val password : String = null, tenant: String, headers: util.Map[String, String],
    override val requireSSL : SSLSupport = SSLWithDefaultRandom, override val reqAuthenticate: Boolean = false, val useTunnel : Boolean = false,
                    override val proxyHostName: String = System.getProperty(TENANT_PROXY_HOST),
                    override val proxyPort : Int =Option(System.getProperty(TENANT_PROXY_PORT)).map(_.toInt).getOrElse(-1),
                    override val proxyUser : String = System.getProperty(TENANT_PROXY_USER),
                    override val proxyPassword: String = System.getProperty(TENANT_PROXY_PWD)) extends HttpRestClient with JsonUtil with AutoCloseable {

  protected def this(baseUrl : String, tenant: String, headers: util.Map[String, String]) = this(baseUrl,  "", "", tenant, headers: util.Map[String, String],
    SSLWithDefaultRandom, false, false)
  
  private final val MESSAGE_SIZE = Option(System.getProperty(HTTP_PAYLOAD_SIZE)).map(_.toInt).getOrElse(Option(System.getenv(HTTP_PAYLOAD_SIZE)).map(_.toInt).getOrElse(5000))
  private final val RETRY_SIZE: Int = Option(System.getProperty(HTTP_RETRY_COUNT)).map(_.toInt).getOrElse(Option(System.getenv(HTTP_RETRY_COUNT)).map(_.toInt).getOrElse(5))
  private final val RETRY_INTERVAL: Int = Option(System.getProperty(HTTP_RETRY_INTERVAL)).map(_.toInt).getOrElse(Option(System.getenv(HTTP_RETRY_INTERVAL)).map(_.toInt).getOrElse(20000))

  private final val dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
	protected final val  SCHEMA_URL: String = "/schema/update/"
  protected final val  INIT_PAYLOAD = "1/1"
	protected final val  JSON_ROOT_TAG: String = "result"
	protected final val  SHEMA: String = "schema"
	protected final val  JSON_COLUMN_TAG: String = "columns"
  protected final val  JSON_TABLE_TAG: String = "table"  
 // override protected val reqAuthenticate: Boolean = false
  protected final val HOST_IP = getLocalIP
  private var count: Int = 0
  
  private def withHeaders[T <: HttpRequestBase](req : T) : T =
    req.withHeaderParams(collection.immutable.HashMap(headers.asScala.toSeq:_*))
   
  
  override implicit  protected def toHttpPost(uri: URI): HttpPost = withHeaders(super.toHttpPost(uri))
  
  def toColumnsJson(schema: Schema): JSONObject = {
    var tableschema = MetaConverters.toTableSchema(schema)
    var jsono: JSONObject = new JSONObject()
		var ja: JSONArray = new JSONArray();
		for(column <- tableschema.columns){
			var jo: JSONObject = new JSONObject(toString(column));
			ja.put(jo);
		}
		jsono.put(JSON_COLUMN_TAG, ja)
        jsono.put(JSON_TABLE_TAG, tableschema.table)  
		jsono
	}
  
  def getDateTimeString(d: Date): String = {
      var calendar = Calendar.getInstance();	    
	    dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
	    dateFormat.format(d);
  }
  
  def getCurrentTimeString(): String = {
	    var calendar = Calendar.getInstance();
	    getDateTimeString(calendar.getTime());
	}

	def postPayload(table: String, schema: Schema, iter: util.Iterator[Array[Object]], extratype: String, seqid: String, processtime: String): Unit = {	
	  
	  val coljson: JSONObject = toColumnsJson(schema)  
	  var pt: String = getCurrentTimeString()
    if (!Option(processtime).getOrElse("").isEmpty)
      pt = processtime
	  	 
    var cachedata = new util.ArrayList[Array[Object]]()  
    var totalcount = 0
    @tailrec def write(data : util.Iterator[Array[Object]], tcount : Int) : Unit = {
      if (data.hasNext) {
        cachedata.add(data.next())
        if (cachedata.size() >= MESSAGE_SIZE) {
            var jsona: JSONArray = toJson(cachedata.iterator(), schema); 
            postData(table, coljson, jsona, extratype, seqid, tcount+"/"+(tcount+1), pt)
            totalcount = totalcount + cachedata.size()
            cachedata.clear()
            write(data, tcount + 1)    
          } else {
            write(data, tcount)
          }
        } else {
          if (cachedata.size() > 0) { //send last
            var jsona: JSONArray = toJson(cachedata.iterator(), schema);
            postData(table, coljson, jsona, extratype, seqid, tcount+"/"+(tcount+1), pt)
            info(table+" with no. of payload as "+getPayloadCount());
            setPayloadCount(0)
            totalcount = totalcount + cachedata.size()
            cachedata.clear()            
          } 
          info(table+" of "+tenant +" total count as:"+totalcount)
      }
    }

    iter match {
          case null => postData(table, coljson, new JSONArray(), extratype, seqid, INIT_PAYLOAD, pt)
          case data => write(data, 1)
    }
	}
	
	def getJobId(extra: String): (String, String) = {
	  try{
	    val jo = new JSONObject(extra)
	    val tenantjid = tenant+"["+jo.get("jobid").toString()+"]"
	    (tenantjid, jo.getString("type"))
	  }catch{
	    case t: Throwable => (null, extra)
	  }
	}
	
	def postData(table: String, schema: JSONObject, results: JSONArray, extratype: String, seqid: String, seq: String, pt: String): Unit = {
	  var seqcont:Int = results.length();
	  val (jobidentifier, eatype) = getJobId(extratype)
	  headers.asScala.+= (PAYLOAD_SEQ_ID -> seqid, PAYLOAD_SEQ -> seq, PAYLOAD_SEQ_CONTENT-> seqcont.toString(), TENANT_REQ_HEADER_PAYLOAD_SIZE-> MESSAGE_SIZE.toString())
	  
    var jsono: JSONObject = new JSONObject()
		jsono.put(JSON_ROOT_TAG, results)
		jsono.put(SHEMA, schema)		
		var jsonString: String = jsono.toString()       
		var request: String = s"json/$tenant/$table/$pt/$eatype"
    info("request " + request+" with PAYLOAD_SEQ="+seq)
		var response = withRetry(RETRY_SIZE){
	    postObject[Map[String,AnyRef]](request, collection.immutable.Map.empty[String,String], jsonString) match {
        case out: Map[String,AnyRef] => {
          info("Post payload as: "+out.get("payload"))
          setPayloadCount(count + 1)
        }
        case _ => {
          error("Error: "+ table+" "+seqid)
          throw new Exception("Exception for post request $request with $table and sequence $seqid")
        }
      }
	  }
	}

  def withRetry[T](n: Int)(code: => T): T = {
    var res: Option[T] = None
    var left = n
    while (!res.isDefined) {
      left = left - 1
      try {
        res = Some(code)
      } catch {
        case t: Throwable if left > 0 => {       
          Thread.sleep(RETRY_INTERVAL)
        }
      }
    }
    res.get
  } 
	
	def toJson(iter: util.Iterator[Array[Object]], schema: Schema): JSONArray ={
	  var result: JSONArray = new JSONArray();
	  iter match {
	    case null => result
	    case _ => {
	      	  var tableschema = MetaConverters.toTableSchema(schema)
	          var columns: Seq[ColumnSchema] = tableschema.columns
	          while(iter.hasNext()){
	            var jobj = new JSONObject
	            var i = 0
	            for(o <- iter.next()){
	               jobj.put(columns(i).name, o match {
                    case s: String if s.length >= 0x10000 => s.substring(0, 0x10000)
                    case d: Date if d != null => getDateTimeString(d)
                    case v => v
                  });
	               i = i + 1
	            }	              
	            result.put(jobj)
	          }
	      	  result
	    }	      
	  }
	}	
	
	def setPayloadCount(c: Int): Unit =
	  count = c
	  
	def getPayloadCount(): Int = 
	  count	  
	
  def getLocalIP(): String = {
	  Try{EC2MetadataUtils.getInstanceInfo().getPrivateIp()
	  }.getOrElse(Try{InetAddress.getLocalHost().getHostAddress()
	  }.getOrElse("0.0.0.0"))
	}
	  
	private var closed = false
  override def close(): Unit = synchronized {
    if(!closed) {
      count = 0
      httpclient.close()
      closed = true
    }
  }

}
