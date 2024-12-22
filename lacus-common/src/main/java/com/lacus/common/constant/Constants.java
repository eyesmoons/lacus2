package com.lacus.common.constant;


/**
 * 通用常量信息
 */
public class Constants {

    public static final int KB = 1024;

    public static final int MB = KB * 1024;

    public static final int GB = MB * 1024;

    // http请求
    public static final String HTTP = "http://";

    // https请求
    public static final String HTTPS = "https://";


    public static class Token {
        // 令牌
        public static final String TOKEN_FIELD = "token";

        // 令牌前缀
        public static final String TOKEN_PREFIX = "Bearer ";

        // 登录用户key
        public static final String LOGIN_USER_KEY = "login_user_key";

    }

    public static class Captcha {
        public static final String MATH_TYPE = "math";
        public static final String CHAR_TYPE = "char";
    }

    // 资源映射路径 前缀
    public static final String RESOURCE_PREFIX = "profile";

    public static class UploadSubDir {
        public static final String AVATAR_PATH = "avatar";
        public static final String DOWNLOAD_PATH = "download";
        public static final String UPLOAD_PATH = "upload";
    }

    public static final String DEFAULT_HDFS_CONFIG = "hdfs.defaultFS";
    public static final String HADOOP_USER_CONFIG = "HADOOP_USER_NAME";
    public static final String HDFS_SITE_XML = "hdfs-site.xml";
    public static final String CORE_SITE_XML = "core-site.xml";
    public static final String YARN_SITE_XML = "yarn-site.xml";
    public static final String FLINK_CONF_YAML = "flink-conf.yaml";
    public static final String HDFS_DEFAULT_FS = "fs.defaultFS";
    public static final String HADOOP_RM_STATE_ACTIVE = "ACTIVE";
    public static final String COMMON_PROPERTIES_PATH = "/common.properties";
    public static final String FORMAT_S_S = "%s/%s";
    public static final String FOLDER_SEPARATOR = "/";
    public static final String RESOURCE_TYPE_FILE = "resources";
    public static final String RESOURCE_TYPE_UDF = "udfs";
    public static final String EMPTY_STRING = "";
    public static final String KAFKA_SERVERS = "kafka.bootstrapServers";
    public static final String HADOOP_RESOURCE_MANAGER_HTTPADDRESS_PORT = "yarn.resource.manager.httpaddress.port";
    public static final String YARN_RESOURCEMANAGER_HA_RM_IDS = "yarn.resourcemanager.ha.rm.ids";
    public static final String YARN_APPLICATION_STATUS_ADDRESS = "yarn.application.status.address";
    public static final String YARN_JOB_HISTORY_STATUS_ADDRESS = "yarn.job.history.status.address";
    public static final String YARN_RESTAPI_ADDRESS = "yarn.restapi-address";
    public static final String YARN_NODE_ADDRESS = "yarn.node-address";
    public static final String HADOOP_USER = "hadoop.username";
    public static final String DATA_BASEDIR_PATH = "data.basedir.path";
    public static final String RESOURCE_VIEW_SUFFIXES = "resource.view.suffixs";
    public static final String RESOURCE_VIEW_SUFFIXES_DEFAULT_VALUE = "txt,log,sh,bat,conf,cfg,py,java,sql,xml,hql,properties,json,yml,yaml,ini,js,csv,md";
    public static final String SUDO_ENABLE = "sudo.enable";
    public static final String RESOURCE_STORAGE_TYPE = "resource.storage.type";
    public static final String COMMA = ",";
    public static final String COLON = ":";
    public static final String DOUBLE_SLASH = "//";
    public static final int HTTP_CONNECT_TIMEOUT = 60 * 1000;
    public static final int HTTP_CONNECTION_REQUEST_TIMEOUT = 60 * 1000;
    public static final int SOCKET_TIMEOUT = 60 * 1000;
    public static final String UTF_8 = "UTF-8";
    public static final String JAR = "jar";
    public static final String KERBEROS = "kerberos";
    public static final String JAVA_SECURITY_KRB5_CONF = "java.security.krb5.conf";
    public static final String JAVA_SECURITY_KRB5_CONF_PATH = "java.security.krb5.conf.path";
    public static final String HADOOP_SECURITY_AUTHENTICATION = "hadoop.security.authentication";
    public static final String HADOOP_SECURITY_AUTHENTICATION_STARTUP_STATE = "hadoop.security.authentication.startup.state";
    public static final String LOGIN_USER_KEY_TAB_USERNAME = "login.user.keytab.username";
    public static final String LOGIN_USER_KEY_TAB_PATH = "login.user.keytab.path";
    public static final int MAX_FILE_SIZE = 1024 * 1024 * 1024;
    public static final String UDF = "UDF";

    public static final int RESOURCE_FULL_NAME_MAX_LENGTH = 128;
    public static final String PARAMETER_DATETIME = "system.datetime";
    public static final String PARAMETER_FORMAT_TIME = "yyyyMMddHHmmss";
    public static final String PARAMETER_SHECDULE_TIME = "schedule.time";

    public static final String STANDALONE_FLINK_OPERATION_SERVER = "standaloneFlinkOperationServer";
    public static final String YARN_FLINK_OPERATION_SERVER = "yarnFlinkOperationServer";
    // flink Rest web 地址(Local Cluster模式)
    public static final String FLINK_HTTP_ADDRESS = "flink.rest.http.address";
    // flink Rest web HA 地址(Standalone Cluster模式 支持HA 可以填写多个地址,用;分隔)
    public static final String FLINK_REST_HA_HTTP_ADDRESS = "flink.rest.ha.http.address";
    public static final String FLINK_SQL_JOB_JAR = "flink.sql.job.jar";
    public static final String FLINK_DEFAULT_SAVEPOINT_PATH = "flink.default.savepoint.path";
    public static final String FLINK_HDFS_COLLECTOR_CONF_PATH = "flink.hdfs.collector.conf-path";
    public static final String FLINK_HDFS_COLLECTOR_JOB_JARS_PATH = "flink.hdfs.collector.job-jars-path";
    public static final String FLINK_HDFS_COLLECTOR_JAR_NAME = "flink.hdfs.collector.jar-name";
    public static final String FLINK_HDFS_COLLECTOR_LIB_PATH = "flink.hdfs.collector.lib-path";
    public static final String FLINK_HDFS_DIST_JAR_PATH = "flink.hdfs.dist-jar-path";
    public static final String LACUS_APPLICATION_HOME = "lacus.application.home";
    public static final String FLINK_CLIENT_HOME = "flink.client.home";
    public static final String FLINK_JOB_EXECUTE_HOME = "flink.job.execute.home";
}
