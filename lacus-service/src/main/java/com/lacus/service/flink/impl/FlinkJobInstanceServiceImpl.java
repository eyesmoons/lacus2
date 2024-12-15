package com.lacus.service.flink.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.lacus.dao.flink.entity.FlinkJobInstanceEntity;
import com.lacus.dao.flink.mapper.FlinkJobInstanceMapper;
import com.lacus.enums.FlinkStatusEnum;
import com.lacus.service.flink.IFlinkJobInstanceService;
import org.springframework.stereotype.Service;

/**
 * @author shengyu
 * @date 2024/10/26 17:28
 */
@Service
public class FlinkJobInstanceServiceImpl extends ServiceImpl<FlinkJobInstanceMapper, FlinkJobInstanceEntity> implements IFlinkJobInstanceService {
    @Override
    public void updateStatus(Long instanceId, FlinkStatusEnum flinkStatusEnum) {
        FlinkJobInstanceEntity instance = baseMapper.selectById(instanceId);
        instance.setStatus(flinkStatusEnum);
        baseMapper.updateById(instance);
    }
}
