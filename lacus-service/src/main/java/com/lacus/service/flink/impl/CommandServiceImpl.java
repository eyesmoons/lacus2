package com.lacus.service.flink.impl;

import com.lacus.dao.flink.entity.FlinkJobEntity;
import com.lacus.dao.system.entity.SysEnvEntity;
import com.lacus.enums.DeployModeEnum;
import com.lacus.service.flink.ICommandService;
import com.lacus.service.flink.model.JobRunParamDTO;
import com.lacus.service.system.ISysEnvService;
import com.lacus.utils.PropertyUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import static com.lacus.common.constant.Constants.FLINK_SQL_JOB_JAR;

@Service
@Slf4j
public class CommandServiceImpl implements ICommandService {
    private static final String APP_CLASS_NAME = "com.lacus.flink.FlinkSqlJobApplication";

    @Autowired
    private ISysEnvService envService;

    /**
     * 本地/Standalone Cluster模式
     */
    @Override
    public String buildRunCommandForCluster(JobRunParamDTO jobRunParamDTO, FlinkJobEntity flinkJobEntity, String savepointPath, String address) throws Exception {
        StringBuilder command = new StringBuilder();
        Long envId = flinkJobEntity.getEnvId();
        SysEnvEntity env = envService.getById(envId);
        if (ObjectUtils.isNotEmpty(env)) {
            String config = env.getConfig();
            command.append(config).append("\n");
        }
        command.append(jobRunParamDTO.getFlinkBinPath()).append(" run -d");
        if (StringUtils.isNotEmpty(address)) {
            command.append(" -m ").append(address);
        }

        if (StringUtils.isNotEmpty(savepointPath)) {
            command.append(" -s ").append(savepointPath);
        }

        if (flinkJobEntity.getDeployMode() == DeployModeEnum.STANDALONE) {
            command.append(" ").append(flinkJobEntity.getFlinkRunConfig());
        }

        if (StringUtils.isNotEmpty(flinkJobEntity.getExtJarPath())) {
            String extJarPath = flinkJobEntity.getExtJarPath();
            String[] urlJarsList = extJarPath.split(",");
            for (String url : urlJarsList) {
                command.append(" -C ").append(url.trim());
            }
        }
        switch (flinkJobEntity.getJobType()) {
            case BATCH_SQL:
            case STREAMING_SQL:
                command.append(" -c ").append(APP_CLASS_NAME);
                command.append(" ").append(jobRunParamDTO.getAppHome()).append(PropertyUtils.getString(FLINK_SQL_JOB_JAR));
                command.append(" -sql ").append(jobRunParamDTO.getSqlPath());
                if (StringUtils.isNotEmpty(jobRunParamDTO.getFlinkCheckpointConfig())) {
                    command.append(" -checkpointDir ").append(jobRunParamDTO.getFlinkCheckpointConfig());
                }
                command.append(" -type ").append(flinkJobEntity.getJobType());
                break;
            case JAR:
                command.append(" -c ").append(flinkJobEntity.getMainClassName());
                command.append(" ").append(jobRunParamDTO.getMainJarPath());
                command.append(" ").append(flinkJobEntity.getCustomArgs());
                break;
            default:
                log.warn("不支持的模式 {}", flinkJobEntity.getJobType());
        }
        log.info("runCommand：{}", command);
        return command.toString();
    }

    /**
     * jar并且构建运行命令
     */
    @Override
    public String buildRunCommandForYarnCluster(JobRunParamDTO jobRunParamDTO, FlinkJobEntity flinkJobEntity, String savepointPath) throws Exception {
        StringBuilder command = new StringBuilder();
        Long envId = flinkJobEntity.getEnvId();
        SysEnvEntity env = envService.getById(envId);
        if (ObjectUtils.isNotEmpty(env)) {
            command.append(env.getConfig()).append("\n");
        }
        command.append(jobRunParamDTO.getFlinkBinPath());
        if (DeployModeEnum.YARN_APPLICATION == flinkJobEntity.getDeployMode()) {
            command.append("  run-application -t yarn-application  ");
        } else {
            command.append(" run -d -t yarn-per-job ");
        }

        if (StringUtils.isNotEmpty(savepointPath)) {
            command.append(" -s ").append(savepointPath);
        }
        command.append(" ").append(jobRunParamDTO.getFlinkRunParam());
        command.append(" -Dyarn.application.name=")
                .append(flinkJobEntity.getJobName());

        String extJarPath = flinkJobEntity.getExtJarPath();
        if (StringUtils.isNotEmpty(extJarPath)) {
            String[] extJarPaths = extJarPath.split(";");
            for (String url : extJarPaths) {
                command.append(" -C ").append(url.trim());
            }
        }

        switch (flinkJobEntity.getJobType()) {
            case STREAMING_SQL:
            case BATCH_SQL:
                command.append(" -c ").append(APP_CLASS_NAME);
                command.append(" ").append(jobRunParamDTO.getAppHome()).append(PropertyUtils.getString(FLINK_SQL_JOB_JAR));
                command.append(" -sql ").append(jobRunParamDTO.getSqlPath());
                if (StringUtils.isNotEmpty(jobRunParamDTO.getFlinkCheckpointConfig())) {
                    command.append(" -checkpointDir ").append(jobRunParamDTO.getFlinkCheckpointConfig());
                }
                command.append(" -type ").append(flinkJobEntity.getJobType());
                break;
            case JAR:
                command.append(" -c ").append(flinkJobEntity.getMainClassName());
                command.append(" ").append(jobRunParamDTO.getMainJarPath());
                command.append(" ").append(flinkJobEntity.getCustomArgs());
                break;
            default:
                log.warn("不支持的部署模式 {}", flinkJobEntity.getJobType());
        }
        log.info("runCommand: {}", command);
        return command.toString();
    }

    @Override
    public String buildSavepointCommandForYarn(String jobId, String targetDirectory, String yarnAppId, String flinkHome) {
        return flinkHome + "/bin/flink" + " savepoint " + jobId + " " + targetDirectory + " " + "-yid " + yarnAppId;
    }

    @Override
    public String buildSavepointCommandForCluster(String jobId, String targetDirectory, String flinkHome) {
        return flinkHome + "/bin/flink" + " savepoint " + jobId + " " + targetDirectory + " ";
    }
}
