package com.lacus.domain.flink.job;

import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.lacus.common.core.page.PageDTO;
import com.lacus.common.exception.CustomException;
import com.lacus.dao.flink.entity.FlinkJobEntity;
import com.lacus.domain.flink.job.command.AddFlinkJarJobCommand;
import com.lacus.domain.flink.job.command.AddFlinkSqlJobCommand;
import com.lacus.domain.flink.job.command.UpdateFlinkJarJobCommand;
import com.lacus.domain.flink.job.command.UpdateFlinkSqlJobCommand;
import com.lacus.domain.flink.job.factory.FlinkOperationServerManager;
import com.lacus.domain.flink.job.model.FlinkJobModel;
import com.lacus.domain.flink.job.model.FlinkJobModelFactory;
import com.lacus.domain.flink.job.query.JobPageQuery;
import com.lacus.enums.DeployModeEnum;
import com.lacus.enums.FlinkJobTypeEnum;
import com.lacus.enums.FlinkStatusEnum;
import com.lacus.service.flink.FlinkJobBaseService;
import com.lacus.service.flink.ICommandRpcClientService;
import com.lacus.service.flink.IFlinkJobService;
import com.lacus.service.flink.IFlinkOperationService;
import com.lacus.service.flink.IStandaloneRpcService;
import com.lacus.service.flink.model.StandaloneFlinkJobInfo;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.validation.Valid;

import static com.lacus.common.constant.Constants.DEFAULT_SAVEPOINT_PATH;

/**
 * @author shengyu
 * @date 2024/10/26 17:32
 */
@Slf4j
@Service
public class FlinkJobService {

    @Autowired
    private IFlinkJobService flinkJobService;
    @Autowired
    private IStandaloneRpcService flinkRpcService;
    @Autowired
    private FlinkJobBaseService flinkJobBaseService;
    @Autowired
    private ICommandRpcClientService commandRpcClientService;

    @SuppressWarnings({"unchecked"})
    public PageDTO pageList(@Valid JobPageQuery query) {
        Page<?> page = flinkJobService.page(query.toPage(), query.toQueryWrapper());
        return new PageDTO(page.getRecords(), page.getTotal());
    }

    public FlinkJobModel addFlinkSqlJob(@Valid AddFlinkSqlJobCommand addCommand) {
        FlinkJobModel model = FlinkJobModelFactory.loadFromSqlAddCommand(addCommand, new FlinkJobModel());
        model.checkJobNameUnique(flinkJobService);
        model.insert();
        return model;
    }

    public FlinkJobModel addFlinkJarJob(@Valid AddFlinkJarJobCommand addCommand) {
        FlinkJobModel model = FlinkJobModelFactory.loadFromJarAddCommand(addCommand, new FlinkJobModel());
        model.checkJobNameUnique(flinkJobService);
        model.insert();
        return model;
    }

    public void updateFlinkSqlJob(@Valid UpdateFlinkSqlJobCommand updateCommand) {
        FlinkJobModel model = FlinkJobModelFactory.loadFromSqlUpdateCommand(updateCommand, new FlinkJobModel());
        model.checkJobNameUnique(flinkJobService);
        model.updateById();
    }

    public void updateFlinkJarJob(@Valid UpdateFlinkJarJobCommand updateCommand) {
        FlinkJobModel model = FlinkJobModelFactory.loadFromJarUpdateCommand(updateCommand, new FlinkJobModel());
        model.checkJobNameUnique(flinkJobService);
        model.updateById();
    }

    public void deleteFlinkJob(Long jobId) {
        flinkJobService.removeById(jobId);
    }

    public FlinkJobEntity detail(Long jobId) {
        return flinkJobService.getById(jobId);
    }

    public void start(Long jobId, Boolean resume) {
        try {
            IFlinkOperationService flinkOperationServer = getFlinkOperationServer(jobId);
            flinkOperationServer.start(jobId, resume);
        } catch (Exception e) {
            throw new CustomException(String.format("flink任务[%s]启动失败：%s", jobId, e.getMessage()));
        }
    }

    /**
     * 根据部署模式获取Flink操作类
     *
     * @param jobId 任务id
     */
    private IFlinkOperationService getFlinkOperationServer(Long jobId) {
        FlinkJobEntity byId = flinkJobService.getById(jobId);
        if (byId == null) {
            throw new CustomException(String.format("任务[%s]不存在", jobId));
        }
        return FlinkOperationServerManager.getFlinkOperationServer(byId.getDeployMode());
    }

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
                if (StringUtils.isNotBlank(flinkJobEntity.getSavepoint()) && flinkJobEntity.getJobType() != FlinkJobTypeEnum.FLINK_SQL_BATCH && FlinkStatusEnum.RUNNING.name().equals(jobStandaloneInfo.getState())) {
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
            throw new CustomException("yarn集群上没有找到对应任务");
        }

        //1、 执行savepoint
        try {
            //yarn模式下和集群模式下统一目录是hdfs:///flink/savepoint/flink-streaming-platform-web/
            //LOCAL模式本地模式下保存在flink根目录下
            String targetDirectory = DEFAULT_SAVEPOINT_PATH + jobId;
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