package com.lacus.api.controller.flink;

import com.lacus.common.core.dto.ResponseDTO;
import com.lacus.common.core.page.PageDTO;
import com.lacus.dao.flink.entity.FlinkJobInstanceEntity;
import com.lacus.domain.flink.instance.FlinkJobInstanceService;
import com.lacus.domain.flink.instance.query.JobInstancePageQuery;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.validation.Valid;

/**
 * flink任务实例管理
 *
 * @author shengyu
 * @date 2024/12/17 10:22
 */
@Api(value = "flink任务实例管理", tags = {"Flink数据开发"})
@RestController
@RequestMapping("/flink/job/instance")
public class FlinkJobInstanceController {

    @Autowired
    private FlinkJobInstanceService flinkJobInstanceService;

    @ApiOperation("任务分页列表")
    @GetMapping("/list/paging")
    @PreAuthorize("@permission.has('flink:job:list')")
    public ResponseDTO<?> listPaging(@Valid JobInstancePageQuery query) {
        PageDTO page = flinkJobInstanceService.pageList(query);
        return ResponseDTO.ok(page);
    }

    @ApiOperation("查看flink任务实例详情")
    @GetMapping("/{instanceId}")
    @PreAuthorize("@permission.has('flink:job:list')")
    public ResponseDTO<FlinkJobInstanceEntity> detail(@PathVariable("instanceId") Long instanceId) {
        FlinkJobInstanceEntity detail = flinkJobInstanceService.detail(instanceId);
        return ResponseDTO.ok(detail);
    }
}
