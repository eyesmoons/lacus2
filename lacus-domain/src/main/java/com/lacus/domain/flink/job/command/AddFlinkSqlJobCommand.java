package com.lacus.domain.flink.job.command;

import lombok.Data;

import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

/**
 * @author shengyu
 * @date 2024/9/13 17:54
 */
@Data
public class AddFlinkSqlJobCommand {

    @NotEmpty(message = "任务名称不能为空")
    private String jobName;

    @NotEmpty(message = "任务类型不能为空")
    private String jobType;

    @NotEmpty(message = "部署模式不能为空")
    private String deployMode;

    @NotEmpty(message = "sql不能为空")
    private String flinkSql;

    private String extJarPath;

    private String flinkRunConfig;

    @NotNull(message = "环境不能为空")
    private Long envId;

    private String remark;
}