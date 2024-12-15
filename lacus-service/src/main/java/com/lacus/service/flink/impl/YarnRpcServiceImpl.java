package com.lacus.service.flink.impl;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.lacus.common.exception.CustomException;
import com.lacus.enums.FlinkStatusEnum;
import com.lacus.service.flink.IYarnRpcService;
import com.lacus.service.flink.model.AppTO;
import com.lacus.service.flink.model.YarnAppInfo;
import com.lacus.service.flink.model.YarnJobInfo;
import com.lacus.utils.PropertyUtils;
import com.lacus.utils.RestUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import static com.lacus.common.constant.Constants.YARN_RESTAPI_ADDRESS;

@Service
@Slf4j
public class YarnRpcServiceImpl implements IYarnRpcService {

    private final RestUtil restUtil;

    public YarnRpcServiceImpl(RestUtil restUtil) {
        this.restUtil = restUtil;
    }

    @Override
    public String getAppIdByYarn(String jobName, String queueName) {
        if (StringUtils.isEmpty(jobName)) {
            throw new CustomException("任务名称为空！");
        }
        String url = getYarnRmHttpAddress() + "ws/v1/cluster/apps?queue=" + queueName;
        String res = restUtil.getForString(url);
        YarnAppInfo yarnAppInfo = JSON.parseObject(res, YarnAppInfo.class);

        this.check(yarnAppInfo, queueName, jobName, url);

        for (AppTO appTO : yarnAppInfo.getApps().getApp()) {
            if (jobName.equals(appTO.getName())) {
                if (FlinkStatusEnum.RUNNING.name().equals(appTO.getState())) {
                    log.info("任务信息：{}", appTO);
                    return appTO.getId();
                } else {
                    log.error("任务运行状态失败，状态为： {}", appTO.getState());
                }
            }
        }
        throw new CustomException("yarn队列" + queueName + "中没有找到运行的任务，jobName：" + jobName);
    }

    public String getYarnRmHttpAddress() {
        String urlHa = PropertyUtils.getString(YARN_RESTAPI_ADDRESS);
        if (StringUtils.isEmpty(urlHa)) {
            throw new CustomException("请配置环境变量[" + YARN_RESTAPI_ADDRESS + "]");
        }
        return getActiveYarnUrl(urlHa);
    }

    private String getActiveYarnUrl(String urlHa) {
        String[] urls = urlHa.split(";");
        for (String http : urls) {
            try {
                String url = http + "ws/v1/cluster/info";
                String res = restUtil.getForString(url);
                if (StringUtils.isNotEmpty(res)) {
                    JSONObject jsonObject = (JSONObject) JSON.parse(res);
                    String haState = jsonObject.getJSONObject("clusterInfo").get("haState").toString();
                    if ("ACTIVE".equalsIgnoreCase(haState)) {
                        return http;
                    }
                }
            } catch (Exception e) {
                log.error("http请求异常：{}", http, e);
            }
        }
        throw new CustomException("网络异常：" + urlHa);
    }

    @Override
    public void stopJobByJobId(String appId) {
        log.info("[stop job]appId：{}", appId);
        if (StringUtils.isEmpty(appId)) {
            throw new CustomException("appId为空");
        }
        String url = getYarnRmHttpAddress() + "ws/v1/cluster/apps/" + appId + "/state";
        log.info("[stop job]请求url：{}", url);
        JSONObject params = new JSONObject();
        params.put("state", "KILLED");
        JSONObject result = restUtil.postForJsonObject(url, params);
        log.info("[stop job]请求结果：{}", result);
    }

    @Override
    public FlinkStatusEnum getJobStateByJobId(String appId) {
        if (StringUtils.isEmpty(appId)) {
            throw new CustomException("appId为空");
        }
        String url = getYarnRmHttpAddress() + "ws/v1/cluster/apps/" + appId + "/state";
        String res = restUtil.getForString(url);
        if (StringUtils.isEmpty(res)) {
            throw new CustomException("请求失败，返回结果为空：" + url);
        }
        return FlinkStatusEnum.getFlinkStateEnum(String.valueOf(JSON.parseObject(res).get("state")));
    }

    @Override
    public YarnJobInfo getJobInfoForPerYarnByAppId(String appId) {
        if (StringUtils.isEmpty(appId)) {
            throw new CustomException("appId为空");
        }
        String res;
        try {
            String url = getYarnRmHttpAddress() + "/proxy/" + appId + "/jobs";
            res = restUtil.getForString(url);
            if (StringUtils.isEmpty(res)) {
                return null;
            }
            JSONArray jsonArray = (JSONArray) JSON.parseObject(res).get("jobs");
            JSONObject jsonObject = (JSONObject) jsonArray.get(0);
            YarnJobInfo jobInfo = new YarnJobInfo();
            jobInfo.setId((String) jsonObject.get("id"));
            jobInfo.setStatus((String) jsonObject.get("status"));
            return jobInfo;
        } catch (Exception e) {
            log.error("get jobInfo for yarn-per-job by appid出错  ", e);
        }
        return null;
    }

    @Override
    public void cancelJobForYarnByAppId(String appId, String jobId) {
        if (StringUtils.isEmpty(appId) || StringUtils.isEmpty(jobId)) {
            throw new CustomException("appId为空");
        }

        String url = getYarnRmHttpAddress() + "/proxy/" + appId + "/jobs/yarn-cancel";
        String res = restUtil.getForString(url);
        log.info("cancel job by appId请求结果：{}", res);
    }


    @Override
    public String getSavepointPath(String appId, String jobId) {
        if (StringUtils.isEmpty(appId) || StringUtils.isEmpty(jobId)) {
            throw new CustomException("appId为空");
        }

        String url = getYarnRmHttpAddress() + "/proxy/" + appId + "/jobs/" + jobId + "/checkpoints";
        String res = restUtil.getForString(url);
        log.info("get savepoint path请求结果: res：{}", res);
        if (StringUtils.isEmpty(res)) {
            return null;
        }
        try {
            JSONObject jsonObject = (JSONObject) JSON.parseObject(res).get("latest");
            JSONObject savepoint = (JSONObject) jsonObject.get("savepoint");
            if (savepoint == null) {
                return null;
            }
            return (String) savepoint.get("external_path");
        } catch (Exception e) {
            log.error("json解析异常{}", res, e);
        }
        return null;
    }

    private void check(YarnAppInfo yarnAppInfo, String queueName, String jobName, String url) {
        if (yarnAppInfo == null) {
            throw new CustomException("yarn队列[" + queueName + "]中没有找到运行的任务：" + jobName);
        }
        if (yarnAppInfo.getApps() == null) {
            throw new CustomException("yarn队列[" + queueName + "]中没有找到运行的任务：" + jobName);
        }
        if (yarnAppInfo.getApps().getApp() == null || yarnAppInfo.getApps().getApp().isEmpty()) {
            throw new CustomException("yarn队列[" + queueName + "]中没有找到运行的任务：" + jobName);
        }
    }
}
