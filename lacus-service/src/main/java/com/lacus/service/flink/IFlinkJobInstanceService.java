package com.lacus.service.flink;

import com.baomidou.mybatisplus.extension.service.IService;
import com.lacus.dao.flink.entity.FlinkJobEntity;
import com.lacus.dao.flink.entity.FlinkJobInstanceEntity;
import com.lacus.enums.FlinkStatusEnum;

public interface IFlinkJobInstanceService extends IService<FlinkJobInstanceEntity> {
    void updateStatus(Long instanceId, FlinkStatusEnum flinkStatusEnum);
}
