package com.lacus.service.flink.impl;

import com.alibaba.fastjson2.JSON;
import com.lacus.common.exception.CustomException;
import com.lacus.enums.DeployModeEnum;
import com.lacus.service.flink.IStandaloneRpcService;
import com.lacus.service.flink.model.StandaloneFlinkJobInfo;
import com.lacus.service.system.ISysConfigService;
import com.lacus.utils.RestUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Service;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.ResourceAccessException;

import java.net.ConnectException;
import java.net.SocketTimeoutException;

import static com.lacus.common.constant.Constants.FLINK_HTTP_ADDRESS;
import static com.lacus.common.constant.Constants.FLINK_REST_HA_HTTP_ADDRESS;

@Slf4j
@Service
public class StandaloneRpcServiceImpl implements IStandaloneRpcService {

    @Autowired
    private ISysConfigService configService;

    @Autowired
    private RestUtil restUtil;

    /**
     * Standalone 模式下获取状态
     */
    public StandaloneFlinkJobInfo getJobInfoForStandaloneByAppId(String appId, DeployModeEnum deployModeEnum) {
        if (ObjectUtils.isNotEmpty(appId)) {
            throw new CustomException("appId不能为空");
        }
        String res = null;
        StandaloneFlinkJobInfo standaloneFlinkJobInfo = null;
        try {
            String flinkHttpAddress = getFlinkHttpAddress(deployModeEnum);
            String url = flinkHttpAddress + "/jobs/" + appId;
            res = restUtil.getForString(url);
            log.info("获取flink任务信息：jobId：{}, url：{}, result：{}", appId, url, res);
            if (ObjectUtils.isEmpty(res)) {
                return null;
            }
            standaloneFlinkJobInfo = JSON.parseObject(res, StandaloneFlinkJobInfo.class);
            return standaloneFlinkJobInfo;
        } catch (HttpClientErrorException e) {
            standaloneFlinkJobInfo = new StandaloneFlinkJobInfo();
            standaloneFlinkJobInfo.setErrors(e.getMessage());
            log.error("json 异常 res：{}", res, e);
        } catch (Exception e) {
            log.error("json 异常 res：{}", res, e);
        }
        return standaloneFlinkJobInfo;
    }

    /**
     * 基于flink rest API取消任务
     */
    public void cancelJobForFlinkByAppId(String jobId, DeployModeEnum deployModeEnum) {
        if (ObjectUtils.isEmpty(jobId)) {
            throw new CustomException("jobId不能为空");
        }
        String flinkHttpAddress = getFlinkHttpAddress(deployModeEnum);
        String url = flinkHttpAddress + "/jobs/" + jobId + "/yarn-cancel";
        String res = restUtil.getForString(url);
        log.info("取消任务：jobId：{}, url：{}, result：{}", jobId, url, res);
    }

    /**
     * 获取savepoint路径
     */
    public String savepointPath(String jobId, DeployModeEnum deployModeEnum) {
        if (ObjectUtils.isEmpty(jobId)) {
            throw new CustomException("jobId为空");
        }
        try {
            String flinkHttpAddress = getFlinkHttpAddress(deployModeEnum);
            String url = flinkHttpAddress + "jobs/" + jobId + "/checkpoints";
            String res = restUtil.getForString(url);
            if (ObjectUtils.isEmpty(res)) {
                return null;
            }
            return JSON.parseObject(res).getJSONObject("latest").getJSONObject("savepoint").getString("external_path");
        } catch (Exception e) {
            log.error("获取 savepoint 出错：{}", e.getMessage());
        }
        return null;
    }

    @Override
    public String getFlinkHttpAddress(DeployModeEnum deployModeEnum) {
        switch (deployModeEnum) {
            case LOCAL:
                String urlLocal = configService.getConfigValueByKey(FLINK_HTTP_ADDRESS);
                if (StringUtils.isEmpty(urlLocal)) {
                    throw new CustomException("flink Rest web 地址为空");
                }
                if (checkUrlConnect(urlLocal)) {
                    return urlLocal.trim();
                }
                throw new CustomException("网络异常 url：" + urlLocal);
            case STANDALONE:
                String urlHA = configService.getConfigValueByKey(FLINK_REST_HA_HTTP_ADDRESS);
                if (StringUtils.isEmpty(urlHA)) {
                    throw new CustomException(FLINK_REST_HA_HTTP_ADDRESS + "为空");
                }
                String[] urls = urlHA.split(";");
                for (String http : urls) {
                    if (checkUrlConnect(http)) {
                        return http.trim();
                    }
                }
                throw new CustomException("网络异常 url：" + urlHA);
            default:
                throw new CustomException("不支持的部署模式");
        }
    }

    public boolean checkUrlConnect(String url) {
        try {
            log.info("connect url：{}", url);
            restUtil.exchangeGet(url, new HttpHeaders(), String.class, new HttpEntity<String>(null, new HttpHeaders()));
        } catch (ResourceAccessException e) {
            if (e.getCause() instanceof ConnectException || e.getCause() instanceof SocketTimeoutException) {
                log.error("网络异常或者超时 url：{}", url, e);
            } else {
                log.warn("检查URL出错： {}", e.getMessage());
            }
            return false;
        } catch (Exception e) {
            log.error("检查URL出错： {}", e.getMessage());
            return false;
        }
        log.info("网络检查成功 url：{}", url);
        return true;
    }
}
