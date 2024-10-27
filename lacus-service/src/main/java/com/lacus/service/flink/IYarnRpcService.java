package com.lacus.service.flink;

import com.lacus.enums.FlinkStatusEnum;
import com.lacus.service.flink.model.YarnJobInfo;

public interface IYarnRpcService {

    /**
     * 通过任务名称获取yarn 的appId
     */
    String getAppIdByYarn(String jobName, String queueName);

    /**
     * 通过http杀掉一个任务
     */
    void stopJobByJobId(String appId);


    /**
     * 查询yarn 上某任务状态
     */
    FlinkStatusEnum getJobStateByJobId(String appId);

    /**
     * per yarn 模式下获取任务新状态
     */
    YarnJobInfo getJobInfoForPerYarnByAppId(String appId);


    /**
     * per yarn 模式下 取消任务
     *
     * @param appId (yarn上的appId)
     * @param jobId (flink上的id)
     */
    void cancelJobForYarnByAppId(String appId, String jobId);

    /**
     * per yarn 模式下 获取SavepointPath 地址
     * 通过checkpoint 接口获取Savepoint地址
     */
    String getSavepointPath(String appId, String jobId);
}
