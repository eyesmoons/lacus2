package com.lacus.api.controller.flink;

import com.lacus.common.core.dto.ResponseDTO;
import com.lacus.common.core.page.PageDTO;
import com.lacus.dao.flink.entity.FlinkJobEntity;
import com.lacus.domain.flink.job.FlinkJobService;
import com.lacus.domain.flink.job.command.AddFlinkJarJobCommand;
import com.lacus.domain.flink.job.command.AddFlinkSqlJobCommand;
import com.lacus.domain.flink.job.command.UpdateFlinkJarJobCommand;
import com.lacus.domain.flink.job.command.UpdateFlinkSqlJobCommand;
import com.lacus.domain.flink.job.query.JobPageQuery;
import com.lacus.enums.FlinkJobTypeEnum;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.validation.Valid;

/**
 * flink任务管理
 *
 * @author shengyu
 * @date 2024/10/26 17:29
 */
@Api(value = "Flink数据开发", tags = {"Flink数据开发"})
@RestController
@RequestMapping("/flink/job")
public class FlinkJobController {

    @Autowired
    private FlinkJobService flinkJobService;

    @ApiOperation("任务分页列表")
    @GetMapping
    @PreAuthorize("@permission.has('flink:job:list')")
    public ResponseDTO<?> jobListPaging(@RequestParam @Valid JobPageQuery query) {
        PageDTO page = flinkJobService.pageList(query);
        return ResponseDTO.ok(page);
    }

    @ApiOperation("新建flink sql流任务")
    @PostMapping("/sql/streaming")
    public ResponseDTO<?> addFlinkSqlStreamingJob(@RequestBody @Valid AddFlinkSqlJobCommand addCommand) {
        addCommand.setJobType(FlinkJobTypeEnum.FLINK_SQL_STREAMING.name());
        return ResponseDTO.ok(flinkJobService.addFlinkSqlJob(addCommand));
    }

    @ApiOperation("新建flink sql批任务")
    @PostMapping("/sql/batch")
    public ResponseDTO<?> addFlinkSqlBatchJob(@RequestBody @Valid AddFlinkSqlJobCommand addCommand) {
        addCommand.setJobType(FlinkJobTypeEnum.FLINK_SQL_BATCH.name());
        return ResponseDTO.ok(flinkJobService.addFlinkSqlJob(addCommand));
    }

    @ApiOperation("新建flink jar任务")
    @PostMapping("/jar")
    public ResponseDTO<?> addFlinkJarJob(@RequestBody @Valid AddFlinkJarJobCommand addCommand) {
        addCommand.setJobType(FlinkJobTypeEnum.FLINK_JAR.name());
        return ResponseDTO.ok(flinkJobService.addFlinkJarJob(addCommand));
    }

    @ApiOperation("编辑flink sql任务")
    @PutMapping("/sql/streaming")
    public ResponseDTO<?> updateFlinkSqlStreamingJob(@RequestBody @Valid UpdateFlinkSqlJobCommand updateCommand) {
        flinkJobService.updateFlinkSqlJob(updateCommand);
        return ResponseDTO.ok();
    }

    @ApiOperation("编辑flink jar任务")
    @PutMapping("/jar")
    public ResponseDTO<?> updateFlinkSqlStreamingJob(@RequestBody @Valid UpdateFlinkJarJobCommand updateCommand) {
        flinkJobService.updateFlinkJarJob(updateCommand);
        return ResponseDTO.ok();
    }

    @ApiOperation("查看flink任务详情")
    @GetMapping("/{jobId}")
    public ResponseDTO<FlinkJobEntity> detail(@PathVariable("jobId") Long jobId) {
        return ResponseDTO.ok(flinkJobService.detail(jobId));
    }

    @ApiOperation("删除flink任务")
    @DeleteMapping("/{jobId}")
    public ResponseDTO<?> deleteFlinkJob(@PathVariable("jobId") Long jobId) {
        flinkJobService.deleteFlinkJob(jobId);
        return ResponseDTO.ok();
    }

    @ApiOperation("启动任务")
    @GetMapping("/start/{jobId}")
    public ResponseDTO<?> start(@PathVariable("jobId") Long jobId) {
        flinkJobService.start(jobId, false);
        return ResponseDTO.ok();
    }

    @ApiOperation("恢复任务")
    @GetMapping("/resume/{jobId}")
    public ResponseDTO<?> resume(@PathVariable("jobId") Long jobId) {
        flinkJobService.start(jobId, true);
        return ResponseDTO.ok();
    }

    @ApiOperation("停止任务")
    @GetMapping("/stop/{jobId}")
    public ResponseDTO<?> stop(@PathVariable("jobId") Long jobId) {
        flinkJobService.stop(jobId, false);
        return ResponseDTO.ok();
    }

    @ApiOperation("暂停任务")
    @GetMapping("/pause/{jobId}")
    public ResponseDTO<?> pause(@PathVariable("jobId") Long jobId) {
        flinkJobService.stop(jobId, true);
        return ResponseDTO.ok();
    }
}