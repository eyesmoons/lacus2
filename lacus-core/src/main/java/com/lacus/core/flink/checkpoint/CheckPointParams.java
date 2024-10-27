package com.lacus.core.flink.checkpoint;

import com.lacus.core.flink.model.CheckPointParam;
import com.lacus.enums.CheckPointParameterEnums;
import com.lacus.enums.StateBackendEnum;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;

@Slf4j
public class CheckPointParams {

    /**
     * 构建checkPoint参数
     */
    public static CheckPointParam buildCheckPointParam(ParameterTool parameterTool) throws Exception {

        String checkpointDir = parameterTool.get(CheckPointParameterEnums.checkpointDir.name(), "");
        //如果checkpointDir为空不启用CheckPoint
        if (StringUtils.isEmpty(checkpointDir)) {
            return null;
        }
        String checkpointingMode = parameterTool.get(CheckPointParameterEnums.checkpointingMode.name(),
                CheckpointingMode.EXACTLY_ONCE.name());

        String checkpointInterval = parameterTool.get(CheckPointParameterEnums.checkpointInterval.name(), "");

        String checkpointTimeout = parameterTool.get(CheckPointParameterEnums.checkpointTimeout.name(), "");

        String tolerableCheckpointFailureNumber = parameterTool.get(CheckPointParameterEnums.tolerableCheckpointFailureNumber.name(), "");

        String asynchronousSnapshots = parameterTool.get(CheckPointParameterEnums.asynchronousSnapshots.name(), "");

        String externalizedCheckpointCleanup = parameterTool.get(CheckPointParameterEnums.externalizedCheckpointCleanup.name(), "");

        String stateBackendType = parameterTool.get(CheckPointParameterEnums.stateBackendType.name(), "");

        String enableIncremental = parameterTool.get(CheckPointParameterEnums.enableIncremental.name(), "");

        CheckPointParam checkPointParam = new CheckPointParam();
        if (StringUtils.isNotEmpty(asynchronousSnapshots)) {
            checkPointParam.setAsynchronousSnapshots(Boolean.parseBoolean(asynchronousSnapshots));
        }
        checkPointParam.setCheckpointDir(checkpointDir);

        checkPointParam.setCheckpointingMode(checkpointingMode);
        if (StringUtils.isNotEmpty(checkpointInterval)) {
            checkPointParam.setCheckpointInterval(Long.valueOf(checkpointInterval));
        }
        if (StringUtils.isNotEmpty(checkpointTimeout)) {
            checkPointParam.setCheckpointTimeout(Long.valueOf(checkpointTimeout));
        }
        if (StringUtils.isNotEmpty(tolerableCheckpointFailureNumber)) {
            checkPointParam
                    .setTolerableCheckpointFailureNumber(Integer.valueOf(tolerableCheckpointFailureNumber));
        }
        if (StringUtils.isNotEmpty(externalizedCheckpointCleanup)) {
            checkPointParam.setExternalizedCheckpointCleanup(externalizedCheckpointCleanup);
        }

        checkPointParam.setStateBackendEnum(StateBackendEnum.getStateBackend(stateBackendType));

        if (StringUtils.isNotEmpty(enableIncremental)) {
            checkPointParam.setEnableIncremental(Boolean.parseBoolean(enableIncremental.trim()));
        }
        log.info("checkPointParam={}", checkPointParam);
        System.out.println("checkPointParam=" + checkPointParam);
        return checkPointParam;
    }
}
