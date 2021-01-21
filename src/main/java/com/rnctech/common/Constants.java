package com.rnctech.common;

/**
 * Constants shared across components
 * Created by Zilin on 1/7/2021.
 */

public class Constants {

	public static final String HTTP_PAYLOAD_SIZE = "Http-Payload-Size";
	public static final String HTTP_RETRY_COUNT = "Http-Retry-Count";
	public static final String HTTP_RETRY_INTERVAL = "Http-Retry-Interval";
	public static final String HTTP_IPADDR_ARGS = "Http-ipaddress";
	
	public static final String TENANT_REQ_HEADER_TENANT_IDENTIFIER = "Rnctech-Tenant-Identifier";
	public static final String TENANT_REQ_HEADER_TENANT_KEY = "Rnctech-Tenant-Key";
	public static final String TENANT_REQ_HEADER_PAYLOAD_SIZE = "Rnctech-Payload-Size";
	public static final String TENANT_REQ_HEADER_SYSINFO = "Rnctech-Tenant-Info";
	public static final String TENANT_REQ_HEADER_NAME = "Rnctech-Tenant-Name";
	public static final String TENANT_REQ_HEADER_LASTEXTRACTTIME = "Rnctech-Tenant-lastExtractTime";

	public static final String TENANT_NAME = "tenant.name";
	public static final String TENANT_URL = "tenant.url";
	public static final String TENANT_KEY = "tenant.key";
	public static final String TENANT_PROXY_HOST = "tenant.proxy.host";
	public static final String TENANT_PROXY_PORT = "tenant.proxy.port";
	public static final String TENANT_PROXY_USER = "tenant.proxy.user";
	public static final String TENANT_PROXY_PWD  = "tenant.proxy.pwd";
	
	public static final String JOB_STATUS_COMPLETED = "COMPLETED";
	public static final String JOB_STATUS_FAILED = "FAILED";
	public static final String JOB_STATUS_COMPLETED_WITH_WARNINGS = "COMPLETED_WITH_WARNINGS";
	public static final String JOB_STATUS_COMPLETEDWITHWARNINGS = "COMPLETEDWITHWARNINGS";
	public static final String JOB_STATUS_COMPLETED_WITH_VERSION_CREATION_FAILURES = "COMPLETEDWITHVERSIONCREATIONFAILURES";
	public static final String JOB_STATUS_ABORTED = "ABORTED";
	public static final String JOB_STATUS_CANCELLED = "CANCELLED";
	public static final String JOB_STATUS_COMPLETED_WITH_ERRORS = "COMPLETED_WITH_ERRORS";
	public static final String JOB_STATUS_COMPLETEDWITHERRORS = "COMPLETEDWITHERRORS";

	public static final String KAFKA_BROKERS = "kafka.brokers";
	public static final String KAFKA_PORTS = "kafka.ports";
	public static final String KAFKA_WRITERS = "kafka.writers";
	
	public static final String SOURCE_HOST = "source.host";
	public static final String SOURCE_USER = "source.host.user";
	public static final String SOURCE_PASSWORD = "source.host.password";
	public static final String SOURCE_PRIVATE_KEY = "source.host.privatekey";
	public static final String SOURCE_PORT = "source.host.port";
	
	public static final String RDBMS_DELCOL = "source.rdbms.delcol";
	public static final String RDBMS_DELEXPR = "source.rdbms.delexpr";
	public static final String RDBMS_LETCOL = "source.rdbms.letcol";
	public static final String RDBMS_CUST_QUERY = "source.rdbms.custom.query";
	public static final String RDBMS_WITH_LET = "source.rdbms.withlet";
	public static final String RDBMS_DBSCHEMA = "source.rdbms.dbschema";
	public static final String RDBMS_JDBC_URL = "source.rdbms.jdbc.url";
	public static final String RDBMS_JDBC_USER = "source.rdbms.jdbc.user";
	public static final String RDBMS_JDBC_PWD = "source.rdbms.jdbc.pwd";
	
	public static final String S3_ACCESS_KEY = "S3_ACCESS_KEY";
	public static final String S3_SECRET_KEY = "S3_SECRET_KEY";
	public static final String S3_REGION = "S3_Region";
	public static final String S3_ROLE_NAME = "S3_RoleName";
	public static final String S3_ROLE_ARN = "S3_RoleArn";
	public static final String S3_USE_IAMROLE = "S3_UseIamRole";
	
	public static final String PAYLOAD_SEQ_ID = "Payload_Seq_Id";  
	public static final String PAYLOAD_SEQ = "Payload_Seq";   // format as "0/88" , ... , "88/88"
	public static final String PAYLOAD_SEQ_CONTENT = "Payload_Seq_Content";  //record count in this payload
	public static final String PAYLOAD_FROM = "Payload_From";	

	public static final String STARTTIME_DEFAULTVALUE = "1970-01-01 00:00:00";
	
	public static enum DATASOURCETYPE {
		salesforce, servicenow, zendesk, zoom, rdbms, jira, agent, internal, unknown
	}

	public static enum DataJobType {
		FULL, INCREMENTAL, SCHEMA, METADATA, PROFILE, CREDENTIALS, SAMPLE
	}

	public static enum REST_METHOD {
		GET, POST, PUT, DELETE
	}
	

}