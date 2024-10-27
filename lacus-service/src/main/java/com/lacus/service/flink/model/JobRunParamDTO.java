package com.lacus.service.flink.model;

import com.lacus.common.exception.CustomException;
import com.lacus.dao.flink.entity.FlinkJobEntity;
import lombok.Data;
import org.apache.commons.lang3.ObjectUtils;

@Data
public class JobRunParamDTO {

    /**
     * flink bin目录地址
     */
    private String flinkBinPath;

    /**
     * flink 运行参数 如：
     */
    private String flinkRunParam;

    /**
     * sql语句存放的目录
     */
    private String sqlPath;

    /**
     * checkpointConfig
     */
    private String flinkCheckpointConfig;

    /**
     * 程序所在目录 如：/use/local/lacus
     */
    private String sysHome;

    /**
     * 主类jar地址
     */
    private String mainJarPath;

    public JobRunParamDTO(String flinkBinPath,
                          String flinkRunParam,
                          String sqlPath,
                          String sysHome,
                          String flinkCheckpointConfig) {
        this.flinkBinPath = flinkBinPath;
        this.flinkRunParam = flinkRunParam;
        this.sqlPath = sqlPath;
        this.sysHome = sysHome;
        this.flinkCheckpointConfig = flinkCheckpointConfig;
    }

    public static JobRunParamDTO buildJobRunParam(FlinkJobEntity flinkJobEntity, String sqlPath) {
        String flinkHome = System.getenv("FLINK_HOME");
        String flinkBinPath = flinkHome + "/bin/flink";
        String flinkRunParam = flinkJobEntity.getFlinkRunConfig();
        String flinkCheckpointPath = System.getenv("FLINK_CHECKPOINT_PATH");
        String sysHome = System.getenv("APP_HOME");

        if (ObjectUtils.isEmpty(flinkHome)) {
            throw new CustomException("请配置环境变量[FLINK_HOME]");
        }
        if (ObjectUtils.isEmpty(flinkCheckpointPath)) {
            throw new CustomException("请配置环境变量[FLINK_CHECKPOINT_PATH]");
        }
        if (ObjectUtils.isEmpty(sysHome)) {
            throw new CustomException("请配置环境变量[APP_HOME]");
        }
        return new JobRunParamDTO(
                flinkBinPath,
                flinkRunParam,
                sqlPath,
                sysHome,
                flinkCheckpointPath
        );
    }
}
