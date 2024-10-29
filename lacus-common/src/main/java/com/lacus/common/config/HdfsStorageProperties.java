package com.lacus.common.config;

import com.lacus.utils.PropertyUtils;
import lombok.Data;
import org.springframework.context.annotation.Configuration;

import static com.lacus.common.constant.Constants.DEFAULT_HDFS_CONFIG;
import static com.lacus.common.constant.Constants.HADOOP_RESOURCE_MANAGER_HTTPADDRESS_PORT;
import static com.lacus.common.constant.Constants.HADOOP_SECURITY_AUTHENTICATION_STARTUP_STATE;
import static com.lacus.common.constant.Constants.HADOOP_USER;
import static com.lacus.common.constant.Constants.YARN_APPLICATION_STATUS_ADDRESS;
import static com.lacus.common.constant.Constants.YARN_JOB_HISTORY_STATUS_ADDRESS;
import static com.lacus.common.constant.Constants.YARN_RESOURCEMANAGER_HA_RM_IDS;

@Data
@Configuration
public class HdfsStorageProperties {

    /**
     * HDFS storage user
     */
    private String user = PropertyUtils.getString(HADOOP_USER);

    /**
     * HDFS default fs
     */
    private String defaultFS = PropertyUtils.getString(DEFAULT_HDFS_CONFIG);

    /**
     * YARN resource manager HA RM ids
     */
    private String yarnResourceRmIds = PropertyUtils.getString(YARN_RESOURCEMANAGER_HA_RM_IDS);

    /**
     * YARN application status address
     */
    private String yarnAppStatusAddress = PropertyUtils.getString(YARN_APPLICATION_STATUS_ADDRESS);

    /**
     * YARN job history status address
     */
    private String yarnJobHistoryStatusAddress = PropertyUtils.getString(YARN_JOB_HISTORY_STATUS_ADDRESS);

    /**
     * Hadoop resource manager http address port
     */
    private String hadoopResourceManagerHttpAddressPort = PropertyUtils.getString(HADOOP_RESOURCE_MANAGER_HTTPADDRESS_PORT);

    /**
     * Hadoop security authentication startup state
     */
    private boolean hadoopSecurityAuthStartupState = PropertyUtils.getBoolean(HADOOP_SECURITY_AUTHENTICATION_STARTUP_STATE, false);
}
