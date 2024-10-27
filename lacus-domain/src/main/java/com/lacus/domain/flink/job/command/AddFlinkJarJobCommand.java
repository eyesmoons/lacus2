package com.lacus.domain.flink.job.command;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import com.lacus.common.core.base.BaseEntity;
import lombok.Data;
import lombok.EqualsAndHashCode;

import javax.validation.constraints.NotEmpty;

/**
 * @author shengyu
 * @date 2024/9/13 17:54
 */
@Data
public class AddFlinkJarJobCommand {

    @NotEmpty(message = "任务名称不能为空")
    private String jobName;

    @NotEmpty(message = "任务类型不能为空")
    private String jobType;

    @NotEmpty(message = "部署模式不能为空")
    private String deployMode;

    @NotEmpty(message = "主类jar包不能为空")
    private String mainJarPath;

    private String extJarPath;

    @NotEmpty(message = "主类名不能为空")
    private String mainClass;

    private String flinkRunConfig;

    private String customArgs;

    @NotEmpty(message = "环境不能为空")
    private Long envId;

    private String remark;
}