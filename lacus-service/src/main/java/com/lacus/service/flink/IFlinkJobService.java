package com.lacus.service.flink;

import com.baomidou.mybatisplus.extension.service.IService;
import com.lacus.dao.flink.entity.FlinkJobEntity;
import com.lacus.enums.FlinkStatusEnum;

public interface IFlinkJobService extends IService<FlinkJobEntity> {
    boolean isJobNameDuplicated(Long jobId, String jobName);

    void updateStatus(Long jobId, FlinkStatusEnum flinkStatusEnum);
}

