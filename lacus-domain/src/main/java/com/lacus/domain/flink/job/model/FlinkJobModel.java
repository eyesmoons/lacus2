package com.lacus.domain.flink.job.model;

import cn.hutool.core.bean.BeanUtil;
import com.lacus.common.exception.ApiException;
import com.lacus.common.exception.error.ErrorCode;
import com.lacus.dao.flink.entity.FlinkJobEntity;
import com.lacus.service.flink.IFlinkJobService;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@EqualsAndHashCode(callSuper = true)
@Data
@NoArgsConstructor
public class FlinkJobModel extends FlinkJobEntity {

    public FlinkJobModel(FlinkJobEntity entity) {
        BeanUtil.copyProperties(entity, this);
    }

    public void checkJobNameUnique(IFlinkJobService jobService) {
        if (jobService.isJobNameDuplicated(getJobId(), getJobName())) {
            throw new ApiException(ErrorCode.Business.JOB_NAME_IS_NOT_UNIQUE, getJobName());
        }
    }
}
