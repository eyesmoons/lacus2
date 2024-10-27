package com.lacus.service.flink;


import com.lacus.enums.DeployModeEnum;

public interface ICommandRpcClientService {


    /**
     * 提交服务
     */
    String submitJob(String command, StringBuilder localLog, DeployModeEnum deployModeEnum) throws Exception;


    /**
     * yarn per模式执行savepoint
     * 默认savepoint保存的地址：hdfs:///flink/savepoint/lacus/
     */
    void savepointForPerYarn(String jobId, String targetDirectory, String yarnAppId) throws Exception;

    /**
     * 集群模式下执行savepoint
     */
    void savepointForPerCluster(String jobId, String targetDirectory) throws Exception;
}
