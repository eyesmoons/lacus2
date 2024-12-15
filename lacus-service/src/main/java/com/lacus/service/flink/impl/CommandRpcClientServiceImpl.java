package com.lacus.service.flink.impl;

import cn.hutool.core.date.DateUtil;
import com.lacus.common.exception.CustomException;
import com.lacus.enums.DeployModeEnum;
import com.lacus.service.flink.FlinkJobBaseService;
import com.lacus.service.flink.ICommandRpcClientService;
import com.lacus.service.flink.ICommandService;
import com.lacus.utils.PropertyUtils;
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

import static com.lacus.common.constant.Constants.FLINK_CLIENT_HOME;

@Slf4j
@Service
public class CommandRpcClientServiceImpl implements ICommandRpcClientService {

    @Autowired
    private ICommandService commandService;

    @Override
    public String submitJob(String command, DeployModeEnum deployModeEnum) throws Exception {
        log.info("任务提交命令：{} ", command);
        Process pcs = Runtime.getRuntime().exec(command);
        //清理错误日志
        this.clearLogStream(pcs.getErrorStream(), String.format("[%s]start local error：[%s]", DateUtil.now(), deployModeEnum.name()));
        String appId = this.clearInfoLogStream(pcs.getInputStream(), deployModeEnum);
        int rs = pcs.waitFor();
        if (rs != 0) {
            throw new RuntimeException("执行异常，appId：" + appId);
        }
        return appId;
    }

    @Override
    public void savepointForPerYarn(String jobId, String targetDirectory, String yarnAppId) throws Exception {
        String command = commandService.buildSavepointCommandForYarn(jobId, targetDirectory, yarnAppId, PropertyUtils.getString(FLINK_CLIENT_HOME));
        log.info("savepoint for yarn command：{}", command);
        this.execSavepoint(command);
    }

    @Override
    public void savepointForPerCluster(String jobId, String targetDirectory) throws Exception {
        String command = commandService.buildSavepointCommandForCluster(jobId, targetDirectory, PropertyUtils.getString(FLINK_CLIENT_HOME));
        log.info("savepoint for per cluster command：{}", command);
        this.execSavepoint(command);
    }

    private void execSavepoint(String command) throws Exception {
        Process pcs = Runtime.getRuntime().exec(command);
        //消费正常日志
        this.clearLogStream(pcs.getInputStream(), "savepoint success！");
        //消费错误日志
        this.clearLogStream(pcs.getErrorStream(), "savepoint error！");
        int rs = pcs.waitFor();
        if (rs != 0) {
            throw new Exception("执行savepoint失败！");
        }
    }

    /**
     * 清理pcs.waitFor()日志
     */
    private void clearLogStream(InputStream stream, final String threadName) {
        WaitForPoolConfigUtil.getInstance().getThreadPoolExecutor().execute(() -> {
                    BufferedReader reader = null;
                    try {
                        Thread.currentThread().setName(threadName);
                        String result;
                        reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
                        while ((result = reader.readLine()) != null) {
                            log.info(result);
                        }
                    } catch (Exception e) {
                        log.error("threadName：{}", threadName);
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
            String result;
            reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
            //按行读取
            while ((result = reader.readLine()) != null) {
                log.info("read：{}", result);
                if (StringUtils.isEmpty(appId) && result.contains("job submitted success:")) {
                    appId = result.replace("job submitted success:", " ").trim();
                    log.info("job submitted success，appId： {}，原始数据：{}", appId, result);
                }
                if (StringUtils.isEmpty(appId) && result.contains("Job has been submitted with JobID")) {
                    appId = result.replace("Job has been submitted with JobID", "").trim();
                    log.info("从日志：{} 中解析到的appId：{}", result, appId);
                }
            }

            if (DeployModeEnum.YARN_APPLICATION == deployModeEnum || DeployModeEnum.YARN_PER == deployModeEnum) {
                log.info("yarn 模式不需要解析appId，可以通过rest接口获取！");
            } else {
                if (StringUtils.isEmpty(appId)) {
                    throw new RuntimeException("解析appId异常！");
                }
            }
            FlinkJobBaseService.APPID_THREAD_LOCAL.set(appId);
            log.info("解析到的appId：{}", appId);
            return appId;
        } catch (CustomException e) {
            throw e;
        } catch (Exception e) {
            log.error("clearInfoLogStream error", e);
            throw new RuntimeException("clearInfoLogStream error");
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
            } catch (IOException e) {
                log.error("[{}] 关闭reader流失败 ", typeName, e);
            }
        }
        if (stream != null) {
            try {
                stream.close();
            } catch (IOException e) {
                log.error("[{}] 关闭stream流失败 ", typeName, e);
            }
        }
        log.info("线程池状态: {}", WaitForPoolConfigUtil.getInstance().getThreadPoolExecutor());
    }
}
