package com.lacus.enums;

import com.lacus.common.exception.CustomException;
import org.apache.commons.lang3.StringUtils;

public enum DeployModeEnum {
    YARN_PER, STANDALONE, LOCAL, YARN_APPLICATION;

    public static DeployModeEnum getModel(String model) {
        if (StringUtils.isEmpty(model)) {
            throw new CustomException("部署模式不能为空");
        }
        for (DeployModeEnum deployModeEnum : DeployModeEnum.values()) {
            if (deployModeEnum.name().equals(model.trim().toUpperCase())) {
                return deployModeEnum;
            }

        }
        throw new CustomException("部署模式不存在");
    }
}
