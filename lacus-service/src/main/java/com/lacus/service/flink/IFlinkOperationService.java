package com.lacus.service.flink;

public interface IFlinkOperationService {

    void start(Long jobId, Boolean resume);

    void stop(Long jobId, Boolean isSavePoint);
}
