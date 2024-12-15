package com.lacus.dao.flink.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import com.lacus.common.core.base.BaseEntity;
import com.lacus.enums.DeployModeEnum;
import com.lacus.enums.FlinkJobTypeEnum;
import com.lacus.enums.FlinkStatusEnum;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * @author shengyu
 * @date 2024/9/13 17:54
 */
@EqualsAndHashCode(callSuper = true)
@TableName("flink_job")
@Data
public class FlinkJobEntity extends BaseEntity<FlinkJobEntity> {

    @TableId(value = "job_id", type = IdType.AUTO)
    private Long jobId;

    @TableField("job_name")
    private String jobName;

    @TableField("app_id")
    private String appId;

    @TableField("save_point")
    private String savepoint;

    @TableField("job_type")
    private FlinkJobTypeEnum jobType;

    @TableField("deploy_mode")
    private DeployModeEnum deployMode;

    @TableField("flink_sql")
    private String flinkSql;

    @TableField("main_jar_path")
    private String mainJarPath;

    @TableField("ext_jar_path")
    private String extJarPath;

    @TableField("main_class")
    private String mainClass;

    @TableField("flink_run_config")
    private String flinkRunConfig;

    @TableField("custom_args")
    private String customArgs;

    @TableField("env_id")
    private Long envId;

    @TableField("job_status")
    private FlinkStatusEnum jobStatus;

    @TableField("remark")
    private String remark;

    @TableField("job_manager")
    private Integer jobManager;

    @TableField("task_manager")
    private Integer taskManager;

    @TableField("slot")
    private Integer slot;

    @TableField("parallelism")
    private Integer parallelism;

    @TableField("queue")
    private String queue;
}
