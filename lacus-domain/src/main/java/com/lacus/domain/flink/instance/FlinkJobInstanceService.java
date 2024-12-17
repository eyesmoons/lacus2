package com.lacus.domain.flink.instance;

import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.lacus.common.core.page.PageDTO;
import com.lacus.dao.flink.entity.FlinkJobEntity;
import com.lacus.dao.flink.entity.FlinkJobInstanceEntity;
import com.lacus.domain.flink.instance.query.JobInstancePageQuery;
import com.lacus.service.flink.IFlinkJobInstanceService;
import com.lacus.service.flink.IFlinkJobService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.validation.Valid;
import java.util.List;

/**
 * @author shengyu
 * @date 2024/12/17 10:31
 */
@Service
public class FlinkJobInstanceService {
    @Autowired
    private IFlinkJobInstanceService flinkJobInstanceService;

    @Autowired
    private IFlinkJobService flinkJobService;

    public PageDTO pageList(@Valid JobInstancePageQuery query) {
        Page<?> page = flinkJobInstanceService.page(query.toPage(), query.toQueryWrapper());
        List<FlinkJobInstanceEntity> records = (List<FlinkJobInstanceEntity>) page.getRecords();
        for (FlinkJobInstanceEntity record : records) {
            Long jobId = record.getJobId();
            FlinkJobEntity byId = flinkJobService.getById(jobId);
            record.setJobName(byId.getJobName());
            record.setJobType(byId.getJobType().name());
        }
        return new PageDTO(records, page.getTotal());
    }

    public FlinkJobInstanceEntity detail(Long instanceId) {
        return flinkJobInstanceService.getById(instanceId);
    }
}
