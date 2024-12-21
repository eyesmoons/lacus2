package com.lacus.flink.enums;

/**
 * @author shengyu
 * @date 2024/10/26 18:03
 */
public enum FlinkJobTypeEnum {
    STREAMING_SQL, BATCH_SQL, JAR;

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
