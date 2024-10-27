package com.lacus.dao.flink.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import com.lacus.common.core.base.BaseEntity;
import com.lacus.enums.FlinkStatusEnum;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.Date;

/**
 * @author shengyu
 * @date 2024/9/13 18:02
 */
@EqualsAndHashCode(callSuper = true)
@TableName("flink_job_instance")
@Data
public class FlinkJobInstanceEntity extends BaseEntity<FlinkJobInstanceEntity> {

    @TableId(value = "instance_id", type = IdType.AUTO)
    private Long instanceId;

    @TableField("job_id")
    private Long jobId;

    @TableField("instance_name")
    private String instanceName;

    @TableField("application_id")
    private String applicationId;

    @TableField("save_point")
    private String savePoint;

    @TableField("flink_job_id")
    private String flinkJobId;

    @TableField("job_script")
    private String jobScript;

    @TableField("submit_time")
    private Date submitTime;

    @TableField("finished_time")
    private Date finishedTime;

    @TableField("status")
    private FlinkStatusEnum status;
}