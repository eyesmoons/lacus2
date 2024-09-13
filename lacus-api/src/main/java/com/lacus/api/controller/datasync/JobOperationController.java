package com.lacus.api.controller.datasync;

import com.lacus.common.core.dto.ResponseDTO;
import com.lacus.core.annotations.AccessLog;
import com.lacus.dao.system.enums.dictionary.BusinessTypeEnum;
import com.lacus.domain.datasync.job.JobOperationService;
import com.lacus.domain.datasync.job.dto.JobSubmitDTO;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Api(value = "任务操作相关接口", tags = {"任务操作相关接口"})
@RestController
@RequestMapping("/datasync/job/operation")
public class JobOperationController {

    @Autowired
    private JobOperationService operationService;

    @ApiOperation("启动任务")
    @PostMapping("/start")
    @AccessLog(title = "任务操作管理", businessType = BusinessTypeEnum.START)
    public ResponseDTO<?> submitJob(@RequestBody JobSubmitDTO jobSubmitDTO) {
        operationService.submitJob(jobSubmitDTO.getJobId(), jobSubmitDTO.getSyncType(), jobSubmitDTO.getTimeStamp());
        return ResponseDTO.ok();
    }

    @ApiOperation("停止任务")
    @GetMapping("/stop/{jobId}")
    @AccessLog(title = "任务操作管理", businessType = BusinessTypeEnum.STOP)
    public ResponseDTO<?> stopJob(@PathVariable("jobId") Long jobId) {
        operationService.stopJob(jobId);
        return ResponseDTO.ok();
    }
}
