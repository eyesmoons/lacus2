package com.lacus.domain.flink.job.command;

import lombok.Data;

/**
 * @author shengyu
 * @date 2024/12/8 14:37
 */
@Data
public class CommonFlinkRunCommand {
    private Integer jobManager;
    private Integer taskManager;
    private Integer slot;
    private Integer parallelism;
    private String queue;
}
