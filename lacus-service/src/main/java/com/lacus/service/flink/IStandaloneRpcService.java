package com.lacus.service.flink;

import com.lacus.enums.DeployModeEnum;
import com.lacus.service.flink.model.StandaloneFlinkJobInfo;

public interface IStandaloneRpcService {

    /**
     * Standalone 模式下获取状态
     */
    StandaloneFlinkJobInfo getJobInfoForStandaloneByAppId(String appId, DeployModeEnum deployModeEnum);

    /**
     * 基于flink rest API取消任务
     */
    void cancelJobForFlinkByAppId(String jobId, DeployModeEnum deployModeEnum);


    /**
     * 获取savepoint路径
     */
    String savepointPath(String jobId, DeployModeEnum deployModeEnum);

    String getFlinkHttpAddress(DeployModeEnum deployModeEnum);
}
