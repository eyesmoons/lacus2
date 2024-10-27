package com.lacus.service.flink;

public interface IFlinkOperationService {

    void start(Long jobId, Boolean resume);

    void resume(Long jobId, String savepoint);

    void pause(Long jobId);

    void stop(Long jobId);
}
