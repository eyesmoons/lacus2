package com.lacus.enums;

/**
 * @author shengyu
 * @date 2024/10/26 22:00
 */
public enum FlinkStatusEnum {
    RUNNING, INITIALIZING, SCHEDULED, FINISHED, STARTING, RESTARTING, FAILED, UNKNOWN, STOP;

    public static FlinkStatusEnum getFlinkStateEnum(String state) {
        for (FlinkStatusEnum stateEnum : FlinkStatusEnum.values()) {
            if (stateEnum.name().equals(state)) {
                return stateEnum;
            }
        }
        return UNKNOWN;
    }
}