package com.lacus.flink;

import com.lacus.flink.checkpoint.CheckPointParams;
import com.lacus.flink.checkpoint.FsCheckPoint;
import com.lacus.flink.enums.FlinkJobTypeEnum;
import com.lacus.flink.executor.ExecuteSql;
import com.lacus.flink.model.JobRunParam;
import com.lacus.flink.parser.SqlFileParser;
import com.lacus.flink.utils.UrlUtils;
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

public class FlinkSqlJobApplication {
    private static final Logger logger = LoggerFactory.getLogger(FlinkSqlJobApplication.class);

    public static void main(String[] args) {
        try {
            Arrays.stream(args).forEach(arg -> logger.info("{}", arg));
            JobRunParam jobRunParam = buildParam(args);
            List<String> fileList;

            if (UrlUtils.isHttpsOrHttp(jobRunParam.getSqlPath())) {
                fileList = UrlUtils.getSqlList(jobRunParam.getSqlPath());
            } else {
                fileList = Files.readAllLines(Paths.get(jobRunParam.getSqlPath()));
            }

            List<String> sqlList = SqlFileParser.parserSql(fileList);
            EnvironmentSettings settings;
            TableEnvironment tEnv;
            if (jobRunParam.getJobTypeEnum() != null && FlinkJobTypeEnum.BATCH_SQL.equals(jobRunParam.getJobTypeEnum())) {
                logger.info("BATCH_SQL任务");
                //批处理
                settings = EnvironmentSettings.newInstance().inBatchMode().build();
                tEnv = TableEnvironment.create(settings);
            } else {
                logger.info("STREAMING_SQL任务");
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
            e.printStackTrace();
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
