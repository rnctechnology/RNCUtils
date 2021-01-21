package com.rnctech.common.utils

import java.io.InputStream
import java.util
import java.util.Date
import org.apache.commons.lang3.StringUtils
import java.text.SimpleDateFormat
import org.joda.time.format.{DateTimeFormatter, ISODateTimeFormat}
import java.util.concurrent.{ CountDownLatch, TimeUnit }
import java.util.concurrent.atomic.AtomicMarkableReference
import com.amazonaws.auth.{BasicAWSCredentials, DefaultAWSCredentialsProviderChain, STSAssumeRoleSessionCredentialsProvider}
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClient
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.event.ProgressEvent
import com.amazonaws.event.ProgressEventType._
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.{ BucketLifecycleConfiguration, ObjectMetadata, Region }
import com.amazonaws.services.s3.transfer.{ TransferManager, Upload }
import scala.util.{Random, Try}

/**
 * S3Client
 * Created by Zilin on 12/17/2020.
 */

class S3Client(val propctx: PropertyContext, val useIAMRole: Boolean = true) extends AutoCloseable with Logging with DateTimeUtil {

  def getCtxProp(key: String, defaultkey: String, defaultVal: String = ""): String = {
    propctx.props.get(key).getOrElse(propctx.props.get(defaultkey).getOrElse(defaultVal)).asInstanceOf[String]
  }
  
  final private val setupPurgeLifecycleRule = true
  final private val transferManager = new AtomicMarkableReference[TransferManager](null, false)
  final private val bucketName = getCtxProp("AWSS3.BUCKET_NAME","bucketName")
  final private val awsKey =getCtxProp("AWSCredentials.ACCESS_KEY","accessKey")
  final private val awsSecretKey = getCtxProp("AWSCredentials.SECRET_KEY","secretKey")    
  final private val credentials = new BasicAWSCredentials(awsKey, awsSecretKey)
  
  final private val regionName = getCtxProp("AWSS3.Region","s3Region","US_WEST_2")
  final private val arn = getCtxProp("AWSCredentials.roleArn","roleArn")
  final private val role = getCtxProp("AWSCredentials.roleSessionName","roleSessionName")
  final private val expireAfterDays: Int = getCtxProp("AWSCredentials.expireAfterDays","expireAfterDays", "7").toInt
  private final val directoryName = getCtxProp("AWSS3.DIR_NAME","dirName")

  
  private lazy val provider = if (useIAMRole) {
    val defaultAWSCredentialsProviderChain = new DefaultAWSCredentialsProviderChain
    defaultAWSCredentialsProviderChain.setReuseLastProvider(true)
    new DefaultAWSCredentialsProviderChain
  } else {
    val configuredRoleSessionName = if (role.length > 48) role.substring(0, 48) else role
    val longLivedCredentials = new BasicAWSCredentials(awsKey, awsSecretKey)
    new STSAssumeRoleSessionCredentialsProvider.Builder(arn, configuredRoleSessionName)
      .withStsClient(new AWSSecurityTokenServiceClient(longLivedCredentials)).build()
  }
  
  @throws[Exception]
  private def getTransferManager = {
    val currentTransferManager = transferManager.getReference
    if (currentTransferManager == null) throw new IllegalStateException("The transfer manager has not been initialized")
    currentTransferManager
  }

  @throws[Exception]
  private def shutdownTransferManager(): Unit = {
    val toShutdownTransferManager = transferManager.getReference
    if ((toShutdownTransferManager != null) && transferManager.compareAndSet(toShutdownTransferManager, null, false, true)) {
      toShutdownTransferManager.abortMultipartUploads(bucketName, new Date(System.currentTimeMillis))
      toShutdownTransferManager.shutdownNow(true)
      transferManager.set(null, false)
    }
  }

  @throws[Exception]
  private def createBucket(s3Client: AmazonS3Client, bucketName: String) = {
    val result = !s3Client.doesBucketExist(bucketName)
    if (result) {
      if (regionName.isEmpty) s3Client.createBucket(bucketName, Region.fromValue(regionName))
      else s3Client.createBucket(bucketName)
    }
    result
  }

  @throws[Exception]
  private def lifeCycleRule(s3Client: AmazonS3Client, bucketName: String, directoryName: String, expireAfterDays: Int): Unit = {
    var bucketLifecycleConfiguration = s3Client.getBucketLifecycleConfiguration(bucketName)
    if (bucketLifecycleConfiguration == null) bucketLifecycleConfiguration = new BucketLifecycleConfiguration
    if (bucketLifecycleConfiguration.getRules == null) bucketLifecycleConfiguration.setRules(new util.ArrayList[BucketLifecycleConfiguration.Rule])
    import scala.collection.JavaConversions._
    if (!bucketLifecycleConfiguration.getRules.exists(_.getPrefix.startsWith(directoryName))) {
      val expireRule = new BucketLifecycleConfiguration.Rule().withId(directoryName + " delete rule")
      bucketLifecycleConfiguration.getRules.add(expireRule.withPrefix(directoryName).withExpirationInDays(expireAfterDays).withStatus(BucketLifecycleConfiguration.ENABLED))
      s3Client.setBucketLifecycleConfiguration(bucketName, bucketLifecycleConfiguration)
    }
  }

  @throws[Exception]
  private def setupTransferManager(): Unit = {
    if (transferManager.compareAndSet(null, null, false, true)) {
      val s3Client = new AmazonS3Client(credentials)
      if (setupPurgeLifecycleRule) lifeCycleRule(s3Client, bucketName, directoryName.split('/')(0), expireAfterDays)
      transferManager.set(new TransferManager(s3Client), false)
    }
  }
  
/*  @throws[Exception] 
  def uploadProfile(profliejson: String): Unit = {
    val s3KeyPrefix: String = getStorageKeyPrefix() + "tables.count.xml"
    Try {
      val contentLength: Int = profliejson.getBytes("UTF-8").length
      val contentType: String = "text/plain"
      open()
      uploadStream(s3KeyPrefix, org.apache.commons.io.IOUtils.toInputStream(profliejson, "UTF-8"), contentLength, contentType)
       s3KeyPrefix
    }
    
  }*/

  @throws[Exception] 
  private def getStorageKeyPrefix() : String =  {
		val dateTimeFormatter: DateTimeFormatter = ISODateTimeFormat.dateHourMinuteSecondMillis().withZoneUTC();
		val randomString: String = StringUtils.upperCase(StringUtils.leftPad(Integer.toHexString(Random.nextInt()), 8, '0'));
		dateTimeFormatter.print(System.currentTimeMillis()) + '/' + randomString + '/';		
	}
  
  @throws[Exception]
  def uploadStream(keyName: String, inputStream: InputStream, contentLength: Int, contentType: String): String = {
    val latch = new CountDownLatch(1)
    var failed = false
    val listener = new com.amazonaws.event.ProgressListener {
      override def progressChanged(progressEvent: ProgressEvent): Unit = {

        progressEvent.getEventType match {
          case TRANSFER_FAILED_EVENT =>
            failed = true
            latch.countDown()
          case TRANSFER_CANCELED_EVENT =>
            latch.countDown()
          case TRANSFER_COMPLETED_EVENT =>
            latch.countDown()
          case _ => Unit

        }
      }
    }
    val objectMetadata = new ObjectMetadata
    objectMetadata.setContentLength(contentLength)
    objectMetadata.setContentType(contentType)
    val currentUpload: Upload = getTransferManager.upload(bucketName, directoryName + "/" + keyName, inputStream, objectMetadata)
    currentUpload.addProgressListener(listener)
    if (!latch.await(20, TimeUnit.SECONDS)) {
      currentUpload.abort()
      warn("upload abort.")
    } else {
      info(s"$bucketName/$directoryName/$keyName")
    }
    s"$bucketName/$directoryName/$keyName"
  }

  @throws[Exception]
  def open(): Unit = {
    this.setupTransferManager()
    createBucket(new AmazonS3Client(credentials), bucketName)
  }

  @throws[Exception]
  override def close(): Unit = {
    this.shutdownTransferManager()
  }

}
