package com.lacus.service.flink.impl;

import com.lacus.dao.flink.entity.FlinkJobEntity;
import com.lacus.dao.flink.entity.FlinkJobInstanceEntity;
import com.lacus.enums.FlinkStatusEnum;
import com.lacus.service.flink.FlinkJobBaseService;
import com.lacus.service.flink.IFlinkJobInstanceService;
import com.lacus.service.flink.IFlinkJobService;
import com.lacus.service.flink.IFlinkOperationService;
import com.lacus.service.flink.IYarnRpcService;
import com.lacus.service.flink.model.JobRunParamDTO;
import com.lacus.service.flink.model.YarnJobInfo;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import static com.lacus.common.constant.Constants.YARN_FLINK_OPERATION_SERVER;

/**
 * @author shengyu
 * @date 2024/10/26 20:45
 */
@Slf4j
@Service(YARN_FLINK_OPERATION_SERVER)
public class YarnFlinkOperationServerImpl implements IFlinkOperationService {

    private static final Integer TRY_TIMES = 2;

    @Autowired
    private IFlinkJobService flinkJobService;

    @Autowired
    private FlinkJobBaseService flinkJobBaseService;

    @Autowired
    private IFlinkJobInstanceService flinkJobInstanceService;

    @Autowired
    private IYarnRpcService yarnRpcService;

    @Override
    public void start(Long jobId, Boolean resume) {

        FlinkJobEntity flinkJobEntity = flinkJobService.getById(jobId);

        //1、检查jobConfigDTO 状态等参数
        flinkJobBaseService.checkStart(flinkJobEntity);

        if (StringUtils.isNotEmpty(flinkJobEntity.getAppId())) {
            this.stop(flinkJobEntity);
        }

        //2、将配置的sql 写入本地文件并且返回运行所需参数
        JobRunParamDTO jobRunParamDTO = flinkJobBaseService.writeSqlToFile(flinkJobEntity);

        //3、保存任务实例
        FlinkJobInstanceEntity instance = new FlinkJobInstanceEntity();
        instance.setJobId(jobId);
        instance.setInstanceName(flinkJobEntity.getJobName() + "_" + System.currentTimeMillis());
        instance.setStatus(FlinkStatusEnum.RUNNING);
        flinkJobInstanceService.save(instance);

        //4、变更任务状态为：启动中
        flinkJobService.updateStatus(jobId, FlinkStatusEnum.RUNNING);

        String savepointPath = null;
        if (resume) {
            savepointPath = flinkJobEntity.getSavepoint();
        }

        //异步提交任务
        flinkJobBaseService.aSyncExecJob(jobRunParamDTO, flinkJobEntity, savepointPath);
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

    private void stop(FlinkJobEntity flinkJobEntity) {
        Integer retryNum = 1;
        while (retryNum <= TRY_TIMES) {
            YarnJobInfo jobInfo = yarnRpcService.getJobInfoForPerYarnByAppId(flinkJobEntity.getAppId());
            log.info("任务[{}]当前状态为：{}", flinkJobEntity.getJobId(), jobInfo);
            if (jobInfo != null && FlinkStatusEnum.RUNNING.name().equals(jobInfo.getStatus())) {
                log.info("执行停止操作 jobYarnInfo={} retryNum={} id={}", jobInfo, retryNum, flinkJobEntity.getJobId());
                yarnRpcService.cancelJobForYarnByAppId(flinkJobEntity.getAppId(), jobInfo.getId());
            } else {
                log.info("任务已经停止 jobYarnInfo={} id={}", jobInfo, flinkJobEntity.getJobId());
                break;
            }
            retryNum++;
        }
    }
}