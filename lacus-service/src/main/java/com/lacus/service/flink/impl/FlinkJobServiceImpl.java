package com.lacus.service.flink.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.lacus.dao.flink.entity.FlinkJobEntity;
import com.lacus.dao.flink.mapper.FlinkJobMapper;
import com.lacus.enums.FlinkStatusEnum;
import com.lacus.service.flink.IFlinkJobService;
import org.springframework.stereotype.Service;

/**
 * @author shengyu
 * @date 2024/10/26 17:26
 */
@Service
public class FlinkJobServiceImpl extends ServiceImpl<FlinkJobMapper, FlinkJobEntity> implements IFlinkJobService {

    @Override
    public boolean isJobNameDuplicated(Long jobId, String jobName) {
        LambdaQueryWrapper<FlinkJobEntity> queryWrapper = new LambdaQueryWrapper<>();
        queryWrapper.ne(jobId != null, FlinkJobEntity::getJobId, jobId);
        queryWrapper.eq(FlinkJobEntity::getJobName, jobName);
        return this.baseMapper.exists(queryWrapper);
    }

    @Override
    public void updateStatus(Long jobId, FlinkStatusEnum flinkStatusEnum) {
        FlinkJobEntity flinkJobEntity = baseMapper.selectById(jobId);
        flinkJobEntity.setJobStatus(flinkStatusEnum);
        baseMapper.updateById(flinkJobEntity);
    }
}