package com.lacus.service.flink.impl;

import cn.hutool.core.date.DateUtil;
import com.lacus.common.exception.CustomException;
import com.lacus.enums.DeployModeEnum;
import com.lacus.service.flink.FlinkJobBaseService;
import com.lacus.service.flink.ICommandRpcClientService;
import com.lacus.service.flink.ICommandService;
import com.lacus.utils.WaitForPoolConfigUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

@Slf4j
@Service
public class CommandRpcClientServiceImpl implements ICommandRpcClientService {

    @Autowired
    private ICommandService commandService;

    @Override
    public String submitJob(String command, DeployModeEnum deployModeEnum) throws Exception {
        log.info(" 任务提交命令是:{} ", command);
        Process pcs = Runtime.getRuntime().exec(command);

        //清理错误日志
        this.clearLogStream(pcs.getErrorStream(), String.format("%s#startForLocal-error#%s", DateUtil.now(), deployModeEnum.name()));
        String appId = this.clearInfoLogStream(pcs.getInputStream(), deployModeEnum);
        int rs = pcs.waitFor();
        if (rs != 0) {
            throw new RuntimeException("执行异常 is error  rs=" + rs);
        }
        return appId;
    }

    @Override
    public void savepointForPerYarn(String jobId, String targetDirectory, String yarnAppId) throws Exception {
        String command = commandService.buildSavepointCommandForYarn(jobId, targetDirectory, yarnAppId, System.getenv("FLINK_HOME"));
        log.info("[savepointForPerYarn] command={}", command);
        this.execSavepoint(command);
    }

    @Override
    public void savepointForPerCluster(String jobId, String targetDirectory) throws Exception {
        String command = commandService.buildSavepointCommandForCluster(jobId, targetDirectory, System.getenv("FLINK_HOME"));
        log.info("[savepointForPerCluster] command：{}", command);
        this.execSavepoint(command);
    }

    private void execSavepoint(String command) throws Exception {
        Process pcs = Runtime.getRuntime().exec(command);
        //消费正常日志
        this.clearLogStream(pcs.getInputStream(), String.format("%s-savepoint-success", DateUtil.now()));
        //消费错误日志
        this.clearLogStream(pcs.getErrorStream(), String.format("%s-savepoint-error", DateUtil.now()));
        int rs = pcs.waitFor();
        if (rs != 0) {
            throw new Exception("[savepointForPerYarn]执行savepoint失败 is error  rs=" + rs);
        }
    }

    /**
     * 清理pcs.waitFor()日志防止死锁
     */
    private void clearLogStream(InputStream stream, final String threadName) {
        WaitForPoolConfigUtil.getInstance().getThreadPoolExecutor().execute(() -> {
                    BufferedReader reader = null;
                    try {
                        Thread.currentThread().setName(threadName);
                        String result = null;
                        reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
                        //按行读取
                        while ((result = reader.readLine()) != null) {
                            log.info(result);
                        }
                    } catch (Exception e) {
                        log.error("threadName={}", threadName);
                    } finally {
                        this.close(reader, stream, "clearLogStream");
                    }
                }
        );
    }

    /**
     * 启动日志输出并且从日志中获取成功后的jobId
     */
    private String clearInfoLogStream(InputStream stream, DeployModeEnum deployModeEnum) {

        String appId = null;
        BufferedReader reader = null;
        try {
            long lastTime = System.currentTimeMillis();
            String result = null;
            reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
            //按行读取
            while ((result = reader.readLine()) != null) {
                log.info("read={}", result);
                if (StringUtils.isEmpty(appId) && result.contains("job-submitted-success:")) {
                    appId = result.replace("job-submitted-success:", " ").trim();
                    log.info("[job-submitted-success] 解析得到的appId是 {}  原始数据 :{}", appId, result);
                }
                if (StringUtils.isEmpty(appId) && result
                        .contains("Job has been submitted with JobID")) {
                    appId = result.replace("Job has been submitted with JobID", "")
                            .trim();
                    log.info("[Job has been submitted with JobID] 解析得到的appId是 {}  原始数据 :{}", appId, result);
                }
            }

            if (DeployModeEnum.YARN_APPLICATION == deployModeEnum
                    || DeployModeEnum.YARN_PER == deployModeEnum) {
                log.info("yarn 模式 不需要获取appId");
            } else {
                if (StringUtils.isEmpty(appId)) {
                    throw new RuntimeException("解析appId异常");
                }
            }
            FlinkJobBaseService.THREADAPPID.set(appId);

            log.info("获取到的appId是 {}", appId);
            return appId;
        } catch (CustomException e) {
            throw e;
        } catch (Exception e) {
            log.error("[clearInfoLogStream] is error", e);
            throw new RuntimeException("clearInfoLogStream is error");
        } finally {
            this.close(reader, stream, "clearInfoLogStream");
        }
    }


    /**
     * 关闭流
     */
    private void close(BufferedReader reader, InputStream stream, String typeName) {
        if (reader != null) {
            try {
                reader.close();
                log.info("[{}]关闭reader ", typeName);
            } catch (IOException e) {
                log.error("[{}] 关闭reader流失败 ", typeName, e);
            }
        }
        if (stream != null) {
            try {
                log.info("[{}]关闭stream ", typeName);
                stream.close();
            } catch (IOException e) {
                log.error("[{}] 关闭stream流失败 ", typeName, e);
            }
        }
        log.info("线程池状态: {}", WaitForPoolConfigUtil.getInstance().getThreadPoolExecutor());
    }
}
