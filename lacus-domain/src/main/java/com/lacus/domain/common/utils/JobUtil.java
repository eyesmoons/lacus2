package com.lacus.domain.common.utils;

import com.lacus.common.constant.Constants;
import com.lacus.common.exception.CustomException;
import com.lacus.dao.datasync.entity.DataSyncJobEntity;
import com.lacus.dao.datasync.entity.DataSyncJobInstanceEntity;
import com.lacus.dao.datasync.entity.DataSyncSavedColumn;
import com.lacus.dao.datasync.entity.DataSyncSavedTable;
import com.lacus.dao.datasync.entity.DataSyncSinkTableEntity;
import com.lacus.dao.datasync.entity.DataSyncSourceTableEntity;
import com.lacus.dao.datasync.enums.FlinkStatusEnum;
import com.lacus.dao.metadata.entity.MetaDatasourceEntity;
import com.lacus.domain.common.dto.JobConf;
import com.lacus.domain.common.dto.JobInfo;
import com.lacus.domain.common.dto.Sink;
import com.lacus.domain.common.dto.SinkDataSource;
import com.lacus.domain.common.dto.Source;
import com.lacus.domain.common.dto.SourceJobConf;
import com.lacus.domain.common.dto.StreamLoadProperty;
import com.lacus.domain.datasync.job.JobMonitorService;
import com.lacus.service.datasync.IDataSyncColumnMappingService;
import com.lacus.service.datasync.IDataSyncJobInstanceService;
import com.lacus.service.datasync.IDataSyncJobService;
import com.lacus.service.datasync.IDataSyncSinkTableService;
import com.lacus.service.datasync.IDataSyncSourceTableService;
import com.lacus.service.datasync.IDataSyncTableMappingService;
import com.lacus.service.metadata.IMetaDataSourceService;
import com.lacus.utils.PropertyUtils;
import com.lacus.utils.hdfs.HdfsUtil;
import com.lacus.utils.time.DateUtils;
import com.lacus.utils.yarn.FlinkConf;
import com.lacus.utils.yarn.YarnUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ObjectUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.lacus.common.constant.Constants.DEFAULT_HDFS_CONFIG;
import static com.lacus.common.constant.Constants.FLINK_HDFS_COLLECTOR_CONF_PATH;
import static com.lacus.common.constant.Constants.FLINK_HDFS_COLLECTOR_JOB_JARS_PATH;
import static com.lacus.common.constant.Constants.KAFKA_SERVERS;

/**
 * @created by shengyu on 2024/2/26 20:59
 */
@Component
@Slf4j
public class JobUtil {

    @Autowired
    IMetaDataSourceService dataSourceService;

    @Autowired
    private IDataSyncSourceTableService sourceTableService;

    @Autowired
    private IDataSyncSinkTableService sinkTableService;

    @Autowired
    private IDataSyncTableMappingService tableMappingService;

    @Autowired
    private IDataSyncColumnMappingService columnMappingService;

    @Autowired
    private IDataSyncJobInstanceService instanceService;

    @Autowired
    private IDataSyncJobService jobService;

    @Autowired
    private JobMonitorService monitorService;

    public JobConf buildJobConf(DataSyncJobEntity job, String syncType, String timeStamp) {
        SourceJobConf sourceJobConf = buildSourceJobConf(job, syncType, timeStamp);
        JobConf jobConf = new JobConf();
        Source source = new Source();
        source.setHostname(sourceJobConf.getHostname());
        source.setDatasourceType(sourceJobConf.getDatasourceType());
        source.setPort(sourceJobConf.getPort());
        source.setUsername(sourceJobConf.getUsername());
        source.setPassword(sourceJobConf.getPassword());
        source.setDatabaseList(sourceJobConf.getDatabaseList());
        source.setTableList(sourceJobConf.getTableList());
        // 处理断点续传
        if (syncType.equalsIgnoreCase("resume")) {
            DataSyncJobInstanceEntity lastInstanceByJobId = instanceService.getLastInstanceByJobId(job.getJobId());
            if (ObjectUtils.isNotEmpty(lastInstanceByJobId) && ObjectUtils.isNotEmpty(lastInstanceByJobId.getFinishedTime())) {
                source.setSyncType("timestamp");
                source.setTimeStamp(convertTimeStamp(lastInstanceByJobId.getFinishedTime()));
            } else {
                throw new CustomException("未找到上次停止位置，请选择其他启动方式");
            }
        } else {
            source.setSyncType(sourceJobConf.getSyncType());
            source.setTimeStamp(sourceJobConf.getTimeStamp());
        }
        source.setSourceName(sourceJobConf.getSourceName());
        source.setBootStrapServers(PropertyUtils.getString(KAFKA_SERVERS));
        source.setTopics(Collections.singletonList(buildTopic(job.getJobId())));
        source.setGroupId(buildGroupId(job.getJobId()));

        Sink sink = new Sink();
        MetaDatasourceEntity sinkMetaDatasource = dataSourceService.getById(job.getSinkDatasourceId());
        List<DataSyncSinkTableEntity> sinkTables = sinkTableService.listByJobId(job.getJobId());

        // 设置sink数据源
        if (ObjectUtils.isNotEmpty(sinkMetaDatasource)) {
            SinkDataSource sinkDataSource = new SinkDataSource();
            sinkDataSource.setDataSourceType(sinkMetaDatasource.getType());
            sinkDataSource.setDataSourceName(sinkMetaDatasource.getDatasourceName());
            sinkDataSource.setIp(sinkMetaDatasource.getIp());
            sinkDataSource.setPort(sinkMetaDatasource.getPort());
            sinkDataSource.setUserName(sinkMetaDatasource.getUsername());
            sinkDataSource.setPassword(sinkMetaDatasource.getPassword());
            if (ObjectUtils.isNotEmpty(sinkTables)) {
                sinkDataSource.setDbName(sinkTables.get(0).getSinkDbName());
            }
            sink.setSinkDataSource(sinkDataSource);
        }

        // 设置stream load属性
        DataSyncSavedTable savedTableQuery = new DataSyncSavedTable();
        savedTableQuery.setJobId(job.getJobId());
        LinkedList<DataSyncSavedTable> savedTables = tableMappingService.listSavedTables(savedTableQuery);
        if (ObjectUtils.isNotEmpty(savedTables)) {
            Map<String, StreamLoadProperty> streamLoadPropertyMap = new HashMap<>();
            for (DataSyncSavedTable savedTable : savedTables) {
                DataSyncSavedColumn savedColumnQuery = convertSavedColumnParams(job, savedTable);
                List<DataSyncSavedColumn> savedColumns = columnMappingService.querySavedColumns(savedColumnQuery);
                List<String> columns = new ArrayList<>();
                List<String> jsonPaths = new ArrayList<>();
                for (DataSyncSavedColumn savedColumn : savedColumns) {
                    columns.add("`" + savedColumn.getSinkColumnName() + "`");
                    jsonPaths.add("\"$." + savedColumn.getSourceColumnName() + "\"");
                }

                StreamLoadProperty streamLoadProperty = new StreamLoadProperty();
                String sourceTable = savedTable.getSourceDbName() + "." + savedTable.getSourceTableName();
                streamLoadProperty.setSinkTable(savedTable.getSinkTableName());
                streamLoadProperty.setColumns(String.join(",", columns));
                streamLoadProperty.setJsonpaths("[" + String.join(",", jsonPaths) + "]");
                streamLoadPropertyMap.put(sourceTable, streamLoadProperty);
            }
            sink.setStreamLoadPropertyMap(streamLoadPropertyMap);
        }

        JobInfo jobInfo = new JobInfo();
        jobInfo.setJobId(job.getJobId());
        jobInfo.setJobName(job.getJobName());

        FlinkConf flinkConf = new FlinkConf();
        flinkConf.setMaxBatchInterval(job.getWindowSize());
        flinkConf.setMaxBatchSize(job.getMaxSize() * 1024 * 1024);
        flinkConf.setMaxBatchRows(job.getMaxCount() * 10000);

        // 设置job信息
        jobConf.setJobInfo(jobInfo);
        // 设置flinkConf
        jobConf.setFlinkConf(flinkConf);
        // 设置source
        jobConf.setSource(source);
        // 设置sink
        jobConf.setSink(sink);
        return jobConf;
    }

    /**
     * 获取最近2分钟的时间戳，防止漏掉数据
     */
    public static Long convertTimeStamp(Date date) {
        if (Objects.isNull(date)) {
            return null;
        }
        Date nowDate = DateUtils.addMinutes(date, -2);
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String beforeDate = format.format(nowDate);
        return format.parse(beforeDate, new ParsePosition(0)).getTime();
    }

    /**
     * 转换映射字段参数查询条件
     *
     * @param job        job
     * @param savedTable savedTable
     */
    private static DataSyncSavedColumn convertSavedColumnParams(DataSyncJobEntity job, DataSyncSavedTable savedTable) {
        DataSyncSavedColumn savedColumnQuery = new DataSyncSavedColumn();
        savedColumnQuery.setJobId(job.getJobId());
        savedColumnQuery.setSourceDatasourceId(job.getSourceDatasourceId());
        savedColumnQuery.setSinkDatasourceId(job.getSinkDatasourceId());
        savedColumnQuery.setSourceDbName(savedTable.getSourceDbName());
        savedColumnQuery.setSinkDbName(savedTable.getSinkDbName());
        savedColumnQuery.setSourceTableName(savedTable.getSourceTableName());
        savedColumnQuery.setSinkTableName(savedTable.getSinkTableName());
        return savedColumnQuery;
    }

    public String buildGroupId(Long jobId) {
        return "rtc_group_" + jobId;
    }

    public String buildTopic(Long jobId) {
        return "rtc_topic_" + jobId;
    }

    /**
     * 构建source任务json
     *
     * @param job       任务信息
     * @param syncType  同步类型
     * @param timeStamp 时间戳
     */
    public SourceJobConf buildSourceJobConf(DataSyncJobEntity job, String syncType, String timeStamp) {
        SourceJobConf sourceJobConf = new SourceJobConf();
        List<DataSyncSourceTableEntity> sourceTables = sourceTableService.listByJobId(job.getJobId());
        sourceJobConf.setJobName(job.getJobName());
        String sourceDbName = sourceTables.get(0).getSourceDbName();
        List<String> sourceTableNames = sourceTables.stream().map(sourceTable -> sourceTable.getSourceDbName() + "." + sourceTable.getSourceTableName()).collect(Collectors.toList());
        MetaDatasourceEntity metaDatasource = dataSourceService.getById(job.getSourceDatasourceId());
        if (ObjectUtils.isNotEmpty(metaDatasource)) {
            sourceJobConf.setBootStrapServer(PropertyUtils.getString(KAFKA_SERVERS));
            sourceJobConf.setTopic(buildTopic(job.getJobId()));
            sourceJobConf.setSourceName(metaDatasource.getDatasourceName());
            sourceJobConf.setDatasourceType(metaDatasource.getType().toLowerCase());
            sourceJobConf.setHostname(metaDatasource.getIp());
            sourceJobConf.setPort(String.valueOf(metaDatasource.getPort()));
            sourceJobConf.setUsername(metaDatasource.getUsername());
            sourceJobConf.setPassword(metaDatasource.getPassword());
            sourceJobConf.setDatabaseList(Collections.singletonList(sourceDbName));
            sourceJobConf.setTableList(sourceTableNames);
            sourceJobConf.setSyncType(syncType);
            if (ObjectUtils.isNotEmpty(timeStamp)) {
                Long afterTimeStamp = DateUtils.dateString2TimeStamp(timeStamp);
                sourceJobConf.setTimeStamp(afterTimeStamp);
            }
        }
        return sourceJobConf;
    }

    /**
     * 设置任务失败状态和结束时间
     */
    public void updateStopStatusForInstance(DataSyncJobInstanceEntity instance) {
        instance.setFinishedTime(new Date());
        instance.setStatus(FlinkStatusEnum.STOP.getStatus());
        instanceService.saveOrUpdate(instance);
    }

    public String convertTaskStatus(String status) {
        String resultStatus = "STOP";
        switch (status) {
            case "INITIALIZING":
            case "RUNNING":
            case "CREATED":
                resultStatus = "RUNNING";
                break;
            case "FAILING":
            case "FAILED":
            case "RESTARTING":
            case "NOINITIATED":
            case "YARN_FAILED":
                resultStatus = "FAILED";
                break;
            case "CANCELLING":
            case "CANCELED":
            case "FINISHED":
            case "STOP":
                resultStatus = "STOP";
                break;
        }
        return resultStatus;
    }

    public String genInstanceNam(String jobName) {
        return jobName + "_" + System.currentTimeMillis();
    }

    public void doStop(Long jobId, Integer type) {
        try {
            DataSyncJobInstanceEntity lastInstance = instanceService.getLastInstanceByJobId(jobId);
            if (ObjectUtils.isNotEmpty(lastInstance)) {
                doStopWithoutSavePoint(lastInstance);
            }
        } catch (Exception e) {
            log.error("任务停止失败：{}", e.getMessage());
        }
    }

    /**
     * 停止任务
     *
     * @param instance 任务实例
     */
    private void doStopWithoutSavePoint(DataSyncJobInstanceEntity instance) {
        DataSyncJobEntity job = jobService.getById(instance.getJobId());
        if (Objects.isNull(job)) {
            throw new CustomException("任务不存在");
        }
        if (!FlinkStatusEnum.couldStop(instance.getStatus())) {
            log.warn("当前状态无法停止：{}", instance.getStatus());
            updateStopStatusForInstance(instance);
        } else {
            String applicationId = instance.getApplicationId();
            String flinkJobId = monitorService.getFlinkJobIdWithRetry(applicationId);
            try {
                for (int i = 0; i < 5; i++) {
                    // 停止flink任务
                    YarnUtil.cancelYarnJob(applicationId, flinkJobId, PropertyUtils.getString(FLINK_HDFS_COLLECTOR_CONF_PATH));
                }
                // 修改任务状态
                updateStopStatusForInstance(instance);
            } catch (Exception e) {
                log.error("flink任务停止失败：", e);
                // 修改任务状态
                updateStopStatusForInstance(instance);
                updateStopStatusForInstance(instance);
                throw new CustomException("flink任务停止失败");
            }
        }
    }

    /**
     * 获取jar包路径
     *
     * @param jarName jar包名称
     */
    public String getJobJarPath(String jarName) {
        String fsPrefix = ObjectUtils.isEmpty(PropertyUtils.getString(DEFAULT_HDFS_CONFIG)) ? Constants.DEFAULT_HDFS_CONFIG : PropertyUtils.getString(DEFAULT_HDFS_CONFIG);
        String hdfsJarPath = fsPrefix + PropertyUtils.getString(FLINK_HDFS_COLLECTOR_JOB_JARS_PATH) + jarName;
        if (!HdfsUtil.exists(hdfsJarPath)) {
            throw new RuntimeException("找不到路径:" + hdfsJarPath);
        }
        return hdfsJarPath;
    }
}