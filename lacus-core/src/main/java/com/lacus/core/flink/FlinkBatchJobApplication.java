package com.lacus.core.flink;

import com.lacus.core.flink.checkpoint.CheckPointParams;
import com.lacus.core.flink.checkpoint.FsCheckPoint;
import com.lacus.core.flink.executor.ExecuteSql;
import com.lacus.core.flink.model.JobRunParam;
import com.lacus.core.flink.parser.SqlFileParser;
import com.lacus.enums.FlinkJobTypeEnum;
import com.lacus.utils.UrlUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

public class FlinkBatchJobApplication {
    private static final Logger logger = LoggerFactory.getLogger(FlinkBatchJobApplication.class);

    public static void main(String[] args) {
        try {
            Arrays.stream(args).forEach(arg -> logger.info("{}", arg));
            JobRunParam jobRunParam = buildParam(args);
            List<String> fileList = null;

            if (UrlUtils.isHttpsOrHttp(jobRunParam.getSqlPath())) {
                fileList = UrlUtils.getSqlList(jobRunParam.getSqlPath());
            } else {
                fileList = Files.readAllLines(Paths.get(jobRunParam.getSqlPath()));
            }

            List<String> sqlList = SqlFileParser.parserSql(fileList);
            EnvironmentSettings settings;
            TableEnvironment tEnv;
            if (jobRunParam.getJobTypeEnum() != null && FlinkJobTypeEnum.FLINK_SQL_BATCH.equals(jobRunParam.getJobTypeEnum())) {
                logger.info("[SQL_BATCH]本次任务是批任务");
                //批处理
                settings = EnvironmentSettings.newInstance().inBatchMode().build();
                tEnv = TableEnvironment.create(settings);
            } else {
                logger.info("[SQL_STREAMING]本次任务是流任务");
                //默认是流 流处理 目的是兼容之前版本
                StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

                settings = EnvironmentSettings.newInstance().inStreamingMode().build();
                tEnv = StreamTableEnvironment.create(env, settings);
                //设置checkPoint
                FsCheckPoint.setCheckpoint(env, jobRunParam.getCheckPointParam());
            }
            JobID jobID = ExecuteSql.exeSql(sqlList, tEnv);
            logger.info("job-submitted-success:{}", jobID);
        } catch (Exception e) {
            System.err.println("任务执行失败:" + e.getMessage());
            logger.error("任务执行失败：", e);
        }
    }

    private static JobRunParam buildParam(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String sqlPath = parameterTool.get("sql");
        if (StringUtils.isEmpty(sqlPath)) {
            throw new NullPointerException("-sql参数 不能为空");
        }
        JobRunParam jobRunParam = new JobRunParam();
        jobRunParam.setSqlPath(sqlPath);
        jobRunParam.setCheckPointParam(CheckPointParams.buildCheckPointParam(parameterTool));
        String type = parameterTool.get("type");
        if (StringUtils.isNotEmpty(type)) {
            jobRunParam.setJobTypeEnum(FlinkJobTypeEnum.getJobTypeEnum(type));
        }
        return jobRunParam;
    }
}
