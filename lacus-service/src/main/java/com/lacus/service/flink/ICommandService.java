package com.lacus.service.flink;

import com.lacus.dao.flink.entity.FlinkJobEntity;
import com.lacus.service.flink.model.JobRunParamDTO;

public interface ICommandService {
    String buildRunCommandForCluster(JobRunParamDTO jobRunParamDTO, FlinkJobEntity flinkJobEntity, String savepointPath, String address) throws Exception;

    String buildRunCommandForYarnCluster(JobRunParamDTO jobRunParamDTO, FlinkJobEntity flinkJobEntity, String savepointPath) throws Exception;

    String buildSavepointCommandForYarn(String jobId, String targetDirectory, String yarnAppId, String flinkHome);

    String buildSavepointCommandForCluster(String jobId, String targetDirectory, String flinkHome);
}
