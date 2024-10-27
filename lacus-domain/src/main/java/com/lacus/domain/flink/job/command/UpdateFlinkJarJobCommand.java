package com.lacus.domain.flink.job.command;

import lombok.Data;
import lombok.EqualsAndHashCode;

import javax.validation.constraints.NotNull;

/**
 * @author shengyu
 * @date 2024/10/26 17:41
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class UpdateFlinkJarJobCommand extends AddFlinkJarJobCommand {
    
    @NotNull(message = "任务id不能为空")
    private Long jobId;
}