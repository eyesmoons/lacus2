package com.lacus.service.flink.impl;

import com.lacus.common.exception.CustomException;
import com.lacus.dao.flink.entity.FlinkJobEntity;
import com.lacus.dao.flink.entity.FlinkJobInstanceEntity;
import com.lacus.enums.FlinkJobTypeEnum;
import com.lacus.enums.FlinkStatusEnum;
import com.lacus.service.flink.FlinkJobBaseService;
import com.lacus.service.flink.IFlinkJobInstanceService;
import com.lacus.service.flink.IFlinkJobService;
import com.lacus.service.flink.IFlinkOperationService;
import com.lacus.service.flink.IStandaloneRpcService;
import com.lacus.service.flink.model.JobRunParamDTO;
import com.lacus.service.flink.model.StandaloneFlinkJobInfo;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Objects;

import static com.lacus.common.constant.Constants.STANDALONE_FLINK_OPERATION_SERVER;

/**
 * @author shengyu
 * @date 2024/10/26 20:44
 */
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

    @Override
    public void start(Long jobId, Boolean resume) {
        FlinkJobEntity byId = flinkJobService.getById(jobId);
        if (ObjectUtils.isNotEmpty(byId.getAppId())) {
            if (!Objects.equals(byId.getJobType(), FlinkJobTypeEnum.FLINK_SQL_BATCH)) {

                StandaloneFlinkJobInfo standaloneFlinkJobInfo = flinkRpcService.getJobInfoForStandaloneByAppId(byId.getAppId(), byId.getDeployMode());
                if (StringUtils.isNotBlank(standaloneFlinkJobInfo.getState()) && FlinkStatusEnum.RUNNING.name().equalsIgnoreCase(standaloneFlinkJobInfo.getState())) {
                    throw new CustomException("Flink任务[" + byId.getAppId() + "]处于[ " + standaloneFlinkJobInfo.getState() + "]状态，不能重复启动任务！");
                }
            }
        }

        //1、检查任务参数
        flinkJobBaseService.checkStart(byId);

        //2、将配置的sql写入本地文件并且返回运行所需参数
        JobRunParamDTO jobRunParamDTO = flinkJobBaseService.writeSqlToFile(byId);

        //3、保存任务实例
        FlinkJobInstanceEntity instance = new FlinkJobInstanceEntity();
        instance.setJobId(jobId);
        instance.setInstanceName(byId.getJobName() + "_" + System.currentTimeMillis());
        instance.setStatus(FlinkStatusEnum.RUNNING);
        flinkJobInstanceService.save(instance);

        //4、变更任务状态：启动中
        flinkJobService.updateStatus(jobId, FlinkStatusEnum.RUNNING);

        String savepointPath = null;
        if (resume) {
            savepointPath = byId.getSavepoint();
        }

        //异步提交任务
        flinkJobBaseService.aSyncExecJob(jobRunParamDTO, byId, savepointPath);
    }

    @Override
    public void resume(Long jobId, String savepoint) {

    }

    @Override
    public void pause(Long jobId) {

    }

    @Override
    public void stop(Long jobId) {

    }
}