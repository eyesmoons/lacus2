package com.lacus.enums;

/**
 * @author shengyu
 * @date 2024/10/26 18:03
 */
public enum FlinkJobTypeEnum {
    FLINK_SQL_STREAMING, FLINK_SQL_BATCH, FLINK_JAR;

    public static FlinkJobTypeEnum getJobTypeEnum(String type) {
        if (type == null) {
            return null;
        }
        for (FlinkJobTypeEnum jobTypeEnum : FlinkJobTypeEnum.values()) {
            if (type.equals(jobTypeEnum.name())) {
                return jobTypeEnum;
            }
        }
        return null;
    }
}