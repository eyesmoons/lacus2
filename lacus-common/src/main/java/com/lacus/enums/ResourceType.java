package com.lacus.enums;

import com.baomidou.mybatisplus.annotation.EnumValue;
import lombok.Getter;

/**
 * resource type
 */
@Getter
public enum ResourceType {

    FILE(0, "file");

    ResourceType(int code, String descp) {
        this.code = code;
        this.descp = descp;
    }

    @EnumValue
    private final int code;
    private final String descp;
}
