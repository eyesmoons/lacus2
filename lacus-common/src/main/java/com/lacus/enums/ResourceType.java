package com.lacus.enums;

import com.baomidou.mybatisplus.annotation.EnumValue;
import lombok.Getter;

/**
 * resource type
 */
@Getter
public enum ResourceType {

    /**
     * 0 file, 1 udf
     */
    FILE(0, "file"),
    UDF(1, "udf"),
    ALL(2, "all");

    ResourceType(int code, String descp) {
        this.code = code;
        this.descp = descp;
    }

    @EnumValue
    private final int code;
    private final String descp;
}
