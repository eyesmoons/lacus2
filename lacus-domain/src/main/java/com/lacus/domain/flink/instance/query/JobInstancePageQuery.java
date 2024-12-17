package com.lacus.domain.flink.instance.query;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.lacus.dao.flink.entity.FlinkJobEntity;
import com.lacus.dao.flink.entity.FlinkJobInstanceEntity;
import com.lacus.dao.system.query.AbstractPageQuery;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.commons.lang3.ObjectUtils;

@EqualsAndHashCode(callSuper = true)
@Data
public class JobInstancePageQuery extends AbstractPageQuery {

    private String jobName;

    private String jobType;

    private Integer jobStatus;

    @Override
    public QueryWrapper<FlinkJobInstanceEntity> toQueryWrapper() {
        QueryWrapper<FlinkJobInstanceEntity> wrapper = new QueryWrapper<>();
        wrapper.like(ObjectUtils.isNotEmpty(jobName), "job_name", jobName);
        wrapper.eq(ObjectUtils.isNotEmpty(jobType), "job_type", jobType);
        wrapper.eq(ObjectUtils.isNotEmpty(jobStatus), "job_status", jobStatus);
        wrapper.orderByDesc("job_id");
        return wrapper;
    }
}
