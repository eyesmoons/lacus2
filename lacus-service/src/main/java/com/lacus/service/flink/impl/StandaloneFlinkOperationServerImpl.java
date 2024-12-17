package com.lacus.service.flink.impl;

import com.lacus.common.exception.CustomException;
import com.lacus.dao.flink.entity.FlinkJobEntity;
import com.lacus.dao.flink.entity.FlinkJobInstanceEntity;
import com.lacus.enums.DeployModeEnum;
import com.lacus.enums.FlinkJobTypeEnum;
import com.lacus.enums.FlinkStatusEnum;
import com.lacus.service.flink.FlinkJobBaseService;
import com.lacus.service.flink.ICommandRpcClientService;
import com.lacus.service.flink.IFlinkJobInstanceService;
import com.lacus.service.flink.IFlinkJobService;
import com.lacus.service.flink.IFlinkOperationService;
import com.lacus.service.flink.IStandaloneRpcService;
import com.lacus.service.flink.model.JobRunParamDTO;
import com.lacus.service.flink.model.StandaloneFlinkJobInfo;
import com.lacus.utils.PropertyUtils;
import com.lacus.utils.time.DateUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Objects;

import static com.lacus.common.constant.Constants.FLINK_DEFAULT_SAVEPOINT_PATH;
import static com.lacus.common.constant.Constants.STANDALONE_FLINK_OPERATION_SERVER;

/**
 * @author shengyu
 * @date 2024/10/26 20:44
 */
@Slf4j
@Service(STANDALONE_FLINK_OPERATION_SERVER)
public class StandaloneFlinkOperationServerImpl implements IFlinkOperationService {

    @Autowired
    private IFlinkJobService flinkJobService;

    @Autowired
    private IStandaloneRpcService flinkRpcService;

    @Autowired
    private FlinkJobBaseService flinkJobBaseService;

    @Autowired
    private IFlinkJobInstanceService flinkJobInstanceService;

    @Autowired
    private ICommandRpcClientService commandRpcClientService;

    @Override
    public void start(Long jobId, Boolean resume) {
        FlinkJobEntity byId = flinkJobService.getById(jobId);
        if (ObjectUtils.isNotEmpty(byId.getAppId())) {
            if (!Objects.equals(byId.getJobType(), FlinkJobTypeEnum.BATCH_SQL)) {
                StandaloneFlinkJobInfo standaloneFlinkJobInfo = flinkRpcService.getJobInfoForStandaloneByAppId(byId.getAppId(), byId.getDeployMode());
                if (StringUtils.isNotBlank(standaloneFlinkJobInfo.getState()) && FlinkStatusEnum.RUNNING.name().equalsIgnoreCase(standaloneFlinkJobInfo.getState())) {
                    throw new CustomException("Flink任务[" + byId.getAppId() + "]处于[ " + standaloneFlinkJobInfo.getState() + "]状态，不能重复启动任务！");
                }
            }
        }

        // 1、检查任务参数
        flinkJobBaseService.checkStart(byId);

        // 2、将配置的sql写入本地文件并且返回运行所需参数
        JobRunParamDTO jobRunParamDTO = flinkJobBaseService.writeSqlToFile(byId);

        // 3、保存任务实例
        FlinkJobInstanceEntity instance = new FlinkJobInstanceEntity();
        instance.setJobId(jobId);
        instance.setDeployMode(byId.getDeployMode());
        instance.setInstanceName(byId.getJobName() + "_" + System.currentTimeMillis());
        instance.setStatus(FlinkStatusEnum.RUNNING);
        instance.setSubmitTime(DateUtils.getNowDate());
        flinkJobInstanceService.save(instance);

        // 4、变更任务状态：启动中
        flinkJobService.updateStatus(jobId, FlinkStatusEnum.RUNNING);

        String savepointPath = null;
        if (resume) {
            savepointPath = byId.getSavepoint();
        }

        // 异步提交任务
        flinkJobBaseService.aSyncExecJob(jobRunParamDTO, byId, instance, savepointPath);
    }

    @Override
    public void stop(Long jobId, Boolean isSavePoint) {
        log.info("开始停止任务[{}]", jobId);
        FlinkJobEntity flinkJobEntity = flinkJobService.getById(jobId);
        if (flinkJobEntity == null) {
            throw new CustomException("任务不存在");
        }
        StandaloneFlinkJobInfo jobStandaloneInfo = flinkRpcService.getJobInfoForStandaloneByAppId(flinkJobEntity.getAppId(), flinkJobEntity.getDeployMode());
        log.info("任务[{}]当前状态为：{}", jobId, jobStandaloneInfo);
        if (jobStandaloneInfo == null || StringUtils.isNotEmpty(jobStandaloneInfo.getErrors())) {
            log.warn("开始停止任务[{}]，getJobInfoForStandaloneByAppId is error jobStandaloneInfo={}", jobId, jobStandaloneInfo);
        } else {
            if (isSavePoint) {
                // 停止前先savepoint
                if (StringUtils.isNotBlank(flinkJobEntity.getSavepoint()) && flinkJobEntity.getJobType() != FlinkJobTypeEnum.BATCH_SQL && FlinkStatusEnum.RUNNING.name().equals(jobStandaloneInfo.getState())) {
                    log.info("开始保存任务[{}]的状态-savepoint", jobId);
                    this.savepoint(jobId);
                }
            }
            //停止任务
            if (FlinkStatusEnum.RUNNING.name().equals(jobStandaloneInfo.getState()) || FlinkStatusEnum.RESTARTING.name().equals(jobStandaloneInfo.getState())) {
                flinkRpcService.cancelJobForFlinkByAppId(flinkJobEntity.getAppId(), flinkJobEntity.getDeployMode());
            }
        }
        //变更状态
        flinkJobService.updateStatus(jobId, FlinkStatusEnum.STOP);
    }

    public void savepoint(Long jobId) {
        FlinkJobEntity flinkJobEntity = flinkJobService.getById(jobId);
        flinkJobBaseService.checkSavepoint(flinkJobEntity);

        StandaloneFlinkJobInfo jobStandaloneInfo = flinkRpcService.getJobInfoForStandaloneByAppId(flinkJobEntity.getAppId(), flinkJobEntity.getDeployMode());
        if (jobStandaloneInfo == null || StringUtils.isNotEmpty(jobStandaloneInfo.getErrors())
                || !FlinkStatusEnum.RUNNING.name().equals(jobStandaloneInfo.getState())) {
            throw new CustomException("集群上没有找到对应任务");
        }

        //1、 执行savepoint
        try {
            //yarn模式下和集群模式下统一目录是hdfs:///flink/savepoint/flink-streaming-platform-web/
            //LOCAL模式本地模式下保存在flink根目录下
            String targetDirectory = PropertyUtils.getString(FLINK_DEFAULT_SAVEPOINT_PATH) + jobId;
            if (DeployModeEnum.LOCAL.equals(flinkJobEntity.getDeployMode())) {
                targetDirectory = "savepoint/" + jobId;
            }
            commandRpcClientService.savepointForPerCluster(flinkJobEntity.getAppId(), targetDirectory);
        } catch (Exception e) {
            throw new CustomException("执行savePoint失败");
        }

        String savepointPath = flinkRpcService.savepointPath(flinkJobEntity.getAppId(), flinkJobEntity.getDeployMode());
        if (StringUtils.isEmpty(savepointPath)) {
            throw new CustomException("没有获取到savepointPath路径目录");
        }
        //2、 保存Savepoint到数据库
        flinkJobEntity.setSavepoint(savepointPath);
        flinkJobService.saveOrUpdate(flinkJobEntity);
    }
}
