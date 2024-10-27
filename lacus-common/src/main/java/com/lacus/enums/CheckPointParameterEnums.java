package com.lacus.enums;

import java.util.Set;

public enum CheckPointParameterEnums {

    checkpointDir,
    checkpointingMode,
    checkpointInterval,
    checkpointTimeout,
    tolerableCheckpointFailureNumber,
    asynchronousSnapshots,
    externalizedCheckpointCleanup,
    stateBackendType,
    enableIncremental;

    public static void isExits(Set<String> keys) {
        for (String key : keys) {
            boolean exits = false;
            for (CheckPointParameterEnums checkPointParameterEnums : CheckPointParameterEnums.values()) {
                if (checkPointParameterEnums.name().equalsIgnoreCase(key)) {
                    exits = true;
                    continue;
                }
            }
            if (!exits) {
                throw new RuntimeException(key + " 暂时不支持");
            }
        }
    }
}
