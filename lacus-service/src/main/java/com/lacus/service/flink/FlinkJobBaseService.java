package com.lacus.service.flink;

import com.lacus.common.exception.CustomException;
import com.lacus.dao.flink.entity.FlinkJobEntity;
import com.lacus.enums.FlinkJobTypeEnum;
import com.lacus.enums.FlinkStatusEnum;
import com.lacus.service.flink.model.JobRunParamDTO;
import com.lacus.service.flink.model.StandaloneFlinkJobInfo;
import com.lacus.utils.CommonThreadPoolUtil;
import com.lacus.utils.file.FileUtil;
import com.lacus.utils.yarn.YarnUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.lacus.enums.DeployModeEnum.YARN_APPLICATION;
import static com.lacus.enums.DeployModeEnum.YARN_PER;

@Component
@Slf4j
public class FlinkJobBaseService {

    public static final ThreadLocal<String> THREADAPPID = new ThreadLocal<String>();

    @Autowired
    private IYarnRpcService IYarnRpcService;
    @Autowired
    private ICommandRpcClientService commandRpcClientService;
    @Autowired
    private IStandaloneRpcService flinkRpcService;
    @Autowired
    private ICommandService commandService;
    @Autowired
    private IFlinkJobService flinkJobService;

    public void checkStart(FlinkJobEntity flinkJobEntity) {
        if (flinkJobEntity == null) {
            throw new CustomException("任务不存在");
        }
        if (FlinkStatusEnum.RUNNING.equals(flinkJobEntity.getJobStatus())) {
            throw new CustomException("任务运行中请先停止任务");
        }
        if (flinkJobEntity.getJobStatus().equals(FlinkStatusEnum.STARTING)) {
            throw new CustomException("任务正在启动中 请稍等..");
        }
        if (Objects.equals(flinkJobEntity.getDeployMode(), YARN_PER) || Objects.equals(flinkJobEntity.getDeployMode(), YARN_APPLICATION)) {
            checkYarnQueue(flinkJobEntity);
        }
    }

    public void checkSavepoint(FlinkJobEntity flinkJobEntity) {
        if (flinkJobEntity == null) {
            throw new CustomException("任务不存在");
        }
        if (FlinkJobTypeEnum.FLINK_SQL_BATCH.equals(flinkJobEntity.getJobType())) {
            throw new CustomException("批任务不支持savePoint：" + flinkJobEntity.getJobName());
        }
        if (StringUtils.isEmpty(flinkJobEntity.getAppId())) {
            throw new CustomException("任务未处于运行状态，不能执行savepoint");
        }
    }

    public JobRunParamDTO writeSqlToFile(FlinkJobEntity flinkJobEntity) {
        String fileName = "flink_sql_job_" + flinkJobEntity.getJobId() + ".sql";
        String sqlPath = System.getenv("APP_HOME") + "/" + fileName;
        FileUtil.writeContent2File(flinkJobEntity.getFlinkSql(), sqlPath);
        return JobRunParamDTO.buildJobRunParam(flinkJobEntity, sqlPath);
    }

    public void aSyncExecJob(JobRunParamDTO jobRunParamDTO, FlinkJobEntity flinkJobEntity, String savepointPath) {
        ThreadPoolExecutor threadPoolExecutor = CommonThreadPoolUtil.getInstance().getThreadPoolExecutor();
        threadPoolExecutor.execute(new Runnable() {
            @Override
            public void run() {
                String appId = "";
                THREADAPPID.set(appId);
                try {
                    String command = "";
                    // 如果是自定义提交jar模式下载文件到本地
                    this.downJar(jobRunParamDTO, flinkJobEntity);
                    switch (flinkJobEntity.getDeployMode()) {
                        case YARN_PER:
                        case YARN_APPLICATION:
                            //1、构建执行命令
                            command = commandService.buildRunCommandForYarnCluster(jobRunParamDTO, flinkJobEntity, savepointPath);
                            //2、提交任务
                            appId = this.submitJobForYarn(command, flinkJobEntity);
                            THREADAPPID.set(appId);
                            break;
                        case LOCAL:
                        case STANDALONE:
                            String address = flinkRpcService.getFlinkHttpAddress(flinkJobEntity.getDeployMode());
                            log.info("flink 远程提交地址是 address={}", address);
                            //1、构建执行命令
                            command = commandService.buildRunCommandForCluster(jobRunParamDTO, flinkJobEntity, savepointPath, address);
                            //2、提交任务
                            appId = this.submitJobForStandalone(command, flinkJobEntity);
                            break;
                        default:
                            log.warn("不支持的模式 {}", flinkJobEntity.getDeployMode());
                    }
                } catch (Exception e) {
                    log.error("任务[{}]执行异常！", flinkJobEntity.getJobId(), e);
                } finally {
                    if (StringUtils.isBlank(appId)) { // 解决任务异常，但已经生成了appID，但没有传递给上层调用方法的问题
                        appId = THREADAPPID.get();
                        log.info("任务[{}]执行异常, appid: {}", flinkJobEntity.getJobId(), appId);
                    }
                    flinkJobService.updateStatus(flinkJobEntity.getJobId(), FlinkStatusEnum.FAILED);
                }
            }

            /**
             *下载文件到本地并且setMainJarPath
             */
            private void downJar(JobRunParamDTO jobRunParamDTO, FlinkJobEntity flinkJobEntity) {
                if (Objects.equals(FlinkJobTypeEnum.FLINK_JAR, flinkJobEntity.getJobType())) {
                    String localJarPath = "";
                    // TODO 从hdfs下载jar包
                    jobRunParamDTO.setMainJarPath(localJarPath);
                }
            }

            private String submitJobForStandalone(String command, FlinkJobEntity flinkJobEntity) throws Exception {
                String appId = commandRpcClientService.submitJob(command, flinkJobEntity.getDeployMode());
                StandaloneFlinkJobInfo jobStandaloneInfo = flinkRpcService.getJobInfoForStandaloneByAppId(appId, flinkJobEntity.getDeployMode());

                if (jobStandaloneInfo == null || StringUtils.isNotEmpty(jobStandaloneInfo.getErrors())) {
                    log.error("[submitJobForStandalone] is error jobStandaloneInfo={}", jobStandaloneInfo);
                    throw new CustomException("任务失败");
                } else {
                    if (!FlinkStatusEnum.RUNNING.name().equals(jobStandaloneInfo.getState())
                            && !FlinkStatusEnum.FINISHED.name().equals(jobStandaloneInfo.getState())) {
                        throw new CustomException("[submitJobForStandalone]任务失败");
                    }
                }
                return appId;
            }

            private String submitJobForYarn(String command, FlinkJobEntity jobConfigDTO) throws Exception {
                commandRpcClientService.submitJob(command, jobConfigDTO.getDeployMode());
                return IYarnRpcService.getAppIdByYarn(jobConfigDTO.getJobName(), YarnUtil.getQueueName(jobConfigDTO.getFlinkRunConfig()));
            }
        });
    }

    private void checkYarnQueue(FlinkJobEntity flinkJobEntity) {
        try {
            String queueName = YarnUtil.getQueueName(flinkJobEntity.getFlinkRunConfig());
            if (StringUtils.isEmpty(queueName)) {
                throw new CustomException("无法获取队列名称，请检查你的 flink运行配置参数");
            }
            String appId = IYarnRpcService.getAppIdByYarn(flinkJobEntity.getJobName(), queueName);
            if (StringUtils.isNotEmpty(appId)) {
                throw new CustomException("该任务在yarn上有运行，请到集群上取消任务后再运行 任务名称是:"
                        + flinkJobEntity.getJobName() + " 队列名称是:" + queueName);
            }
        } catch (CustomException e) {
            throw e;
        } catch (Exception e) {
            throw new CustomException(e.getMessage());
        }
    }

    /**
     * 正则表达式，区配参数：${xxxx}
     */
    private static final Pattern PARAM_PATTERN = Pattern.compile("\\$\\{[\\w.-]+\\}");

    private String replaceParamter(Properties properties, String text) {
        if (text == null) {
            return null;
        }
        Matcher m = PARAM_PATTERN.matcher(text);
        while (m.find()) {
            String param = m.group();
            String key = param.substring(2, param.length() - 1);
            String value = (String) properties.get(key);
            if (value != null) {
                text = text.replace(param, value);
            }
        }
        return text;
    }
}
