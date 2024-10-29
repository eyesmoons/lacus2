package com.lacus.domain.datasync.job;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.google.common.collect.Lists;
import com.lacus.common.exception.ApiException;
import com.lacus.common.exception.error.ErrorCode;
import com.lacus.utils.PropertyUtils;
import com.lacus.utils.RestUtil;
import com.lacus.utils.kafka.KafkaUtil;
import com.lacus.utils.time.DateUtils;
import com.lacus.utils.yarn.ApplicationModel;
import com.lacus.utils.yarn.ConfigUtil;
import com.lacus.utils.yarn.FlinkJobDetail;
import com.lacus.utils.yarn.YarnMetrics;
import com.lacus.utils.yarn.YarnUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.lacus.common.constant.Constants.FLINK_HDFS_COLLECTOR_CONF_PATH;
import static com.lacus.common.constant.Constants.YARN_NODE_ADDRESS;
import static com.lacus.common.constant.Constants.YARN_RESTAPI_ADDRESS;

@Slf4j
@Service("monitorService")
public class JobMonitorService {

    @Autowired
    private RestUtil restUtil;

    private final static String appType = "Apache Flink";

    private final String flinkRestPrefix = PropertyUtils.getString(YARN_RESTAPI_ADDRESS);

    public List<ApplicationModel> listFlinkJob() {
        return YarnUtil.listYarnRunningJob(PropertyUtils.getString(FLINK_HDFS_COLLECTOR_CONF_PATH), appType);
    }

    public YarnMetrics clusterDetail() {
        YarnConfiguration conf = new YarnConfiguration();
        ConfigUtil.initConfig(conf, PropertyUtils.getString(FLINK_HDFS_COLLECTOR_CONF_PATH));
        String host = conf.get("yarn.resourcemanager.webapp.address");
        JSONObject json = restUtil.getForJsonObject("http://" + host + "/ws/v1/cluster/metrics");
        log.debug("返回信息:{}", JSON.toJSONString(json));
        YarnMetrics yarnMetrics = json.getJSONObject("clusterMetrics").toJavaObject(YarnMetrics.class);

        yarnMetrics.setYarnNode(PropertyUtils.getString(YARN_NODE_ADDRESS));
        yarnMetrics.setTotalGB(yarnMetrics.getTotalMB() / 1024);
        yarnMetrics.setAllocatedGB(yarnMetrics.getAllocatedMB() / 1024);
        yarnMetrics.setAvailableGB(yarnMetrics.getAvailableMB() / 1024);
        BigDecimal memoryPer = BigDecimal.valueOf((double) yarnMetrics.getAllocatedMB() / yarnMetrics.getTotalMB());
        BigDecimal corePer = BigDecimal.valueOf((double) yarnMetrics.getAllocatedVirtualCores() / yarnMetrics.getTotalVirtualCores());
        DecimalFormat df = new DecimalFormat("#.####");
        yarnMetrics.setMemoryPercents(df.format(memoryPer));
        yarnMetrics.setVirtualCoresPercents(df.format(corePer));
        return yarnMetrics;
    }

    public ApplicationModel yarnJobDetail(String appId) {
        return YarnUtil.yarnJobDetail(PropertyUtils.getString(FLINK_HDFS_COLLECTOR_CONF_PATH), appId);
    }

    public Object flinkJobOverview(String applicationId) {
        String sb = flinkRestPrefix + applicationId + "/jobs/overview";
        return restUtil.getForObject(sb);
    }

    public FlinkJobDetail getFlinkJobDetailWithRetry(String applicationId, String flinkJobId) {
        FlinkJobDetail flinkJobDetail = null;
        for (int i = 0; i < 5; i++) {
            flinkJobDetail = getFlinkJobDetail(applicationId, flinkJobId);
            if (Objects.nonNull(flinkJobDetail)) {
                break;
            }
        }
        return flinkJobDetail;
    }

    public String getFlinkJobIdWithRetry(String applicationId) {
        String flinkJobId = null;
        for (int i = 0; i < 5; i++) {
            flinkJobId = getFlinkJobId(applicationId);
            if (Objects.nonNull(flinkJobId)) {
                break;
            }
        }
        return flinkJobId;
    }

    private FlinkJobDetail getFlinkJobDetail(String applicationId, String flinkJobId) {
        FlinkJobDetail jobDetail;
        String sb = flinkRestPrefix + applicationId + "/jobs/" + flinkJobId;
        try {
            Object result = restUtil.getForObject(sb);
            while (ObjectUtils.isEmpty(result)) {
                result = restUtil.getForObject(sb);
            }
            jobDetail = JSONObject.parseObject(JSON.toJSONString(result), FlinkJobDetail.class);
        } catch (Exception e) {
            log.error("获取flink application信息失败：{}", e.getMessage());
            return null;
        }
        return jobDetail;
    }

    public String getFlinkJobId(String applicationId) {
        String flinkJobId;
        try {
            String sb = flinkRestPrefix + applicationId + "/jobs/overview";
            JSONObject result = restUtil.getForJsonObject(sb);
            log.info("[{}：尝试获取flink job id]，请求地址：{}；返回信息：{}", applicationId, sb, JSON.toJSONString(result));
            JSONArray jobs = JSON.parseObject(JSON.toJSONString(result)).getJSONArray("jobs");
            int cnt = 0;
            while (ObjectUtils.isEmpty(jobs)) {
                JSONObject result2 = restUtil.getForJsonObject(sb);
                jobs = JSON.parseObject(JSON.toJSONString(result2)).getJSONArray("jobs");
                log.info("{}：重试获取flink job id，当前第[{}]次，结果：{}", applicationId, (++cnt), JSON.toJSONString(result2));
            }
            flinkJobId = jobs.getJSONObject(0).getString("jid");
        } catch (Exception e) {
            log.error("{}：获取flink job id失败：{}", applicationId, e.getMessage());
            return null;
        }
        return flinkJobId;
    }

    public FlinkJobDetail flinkJobDetail(String applicationId) {
        String flinkJobId = getFlinkJobId(applicationId);
        if (Objects.isNull(flinkJobId)) {
            for (int i = 0; i < 3; i++) {
                flinkJobId = getFlinkJobId(applicationId);
            }
        }
        if (Objects.isNull(flinkJobId)) {
            throw new RuntimeException("flinkJobId为null");
        }
        String sb = flinkRestPrefix + applicationId + "/jobs/" + flinkJobId;
        Object forObject = restUtil.getForObject(sb);
        return JSONObject.parseObject(JSON.toJSONString(forObject), FlinkJobDetail.class);
    }

    public Object flinkJobCheckpoint(String applicationId) {
        String flinkJobId = getFlinkJobId(applicationId);
        String sb = flinkRestPrefix + applicationId + "/jobs/" + flinkJobId + "/checkpoints";
        return restUtil.getForObject(sb);
    }

    public Object flinkJobCheckpointConfig(String applicationId) {
        String flinkJobId = getFlinkJobId(applicationId);
        String sb = flinkRestPrefix + applicationId + "/jobs/" + flinkJobId + "/checkpoints/config";
        return restUtil.getForObject(sb);
    }

    public Object flinkJobExceptions(String applicationId, String flinkJobId) {
        StringBuilder sb = new StringBuilder();
        sb.append(flinkRestPrefix)
                .append(applicationId)
                .append("/jobs/")
                .append(flinkJobId)
                .append("/exceptions");
        try {
            return restUtil.getForObject(sb.toString());
        } catch (Exception e) {
            throw new ApiException(ErrorCode.Internal.UNKNOWN_ERROR, "获取异常信息失败");
        }
    }

    public Object kafkaConsumedRate(String applicationId, String flinkJobId) {
        List<String> vertices = vertices(applicationId, flinkJobId);
        if (ObjectUtils.isNotEmpty(vertices)) {
            String ver = vertices.get(0);
            String sb = flinkRestPrefix +
                    applicationId +
                    "/jobs/" +
                    flinkJobId +
                    "/vertices/" +
                    ver +
                    "/subtasks/metrics?get=Source__Custom_Source.KafkaConsumer.records-consumed-rate";
            Object rs = restUtil.getForObject(sb);
            log.debug("rest返回信息:{}", JSON.toJSONString(rs));
            return rs;
        } else {
            throw new ApiException(ErrorCode.Internal.UNKNOWN_ERROR, "获取Flink监控信息失败");
        }
    }

    public Object kafkaConsumedLag(String sinkJobConf, List<String> topics) {
        JSONObject sourceMap = JSON.parseObject(JSON.toJSONString(sinkJobConf));
        Map<String, Long> topicLag = KafkaUtil.kafkaMessageLag(sourceMap, topics);
        String time = DateUtils.getCurrentTime();
        List<Map<String, Object>> result = Lists.newArrayList();
        for (Map.Entry<String, Long> entry : topicLag.entrySet()) {
            Map<String, Object> map = new HashMap<>();
            map.put("topic", entry.getKey());
            map.put("value", entry.getValue());
            map.put("time", time);
            result.add(map);
        }
        return result;
    }

    public Object recordInOutPerSecond(String applicationId, String flinkJobId) {
        List<String> vertices = vertices(applicationId, flinkJobId);
        if (ObjectUtils.isEmpty(vertices)) {
            throw new ApiException(ErrorCode.Internal.UNKNOWN_ERROR, "获取Flink监控信息失败");
        }
        String baseUrl = flinkRestPrefix + applicationId + "/jobs/" + flinkJobId + "/vertices/";
        JSONArray rsLists = new JSONArray();
        for (int i = 0; i < vertices.size(); i++) {
            StringBuilder sbtmp = new StringBuilder();
            sbtmp.append(baseUrl).append(vertices.get(i));
            if (i == 0) {
                sbtmp.append("/subtasks/metrics?get=numRecordsOutPerSecond");
            } else {
                sbtmp.append("/subtasks/metrics?get=numRecordsInPerSecond");
            }
            Object rs = restUtil.getForObject(sbtmp.toString());
            JSONArray rsArray = JSON.parseArray(JSON.toJSONString(rs));
            if (ObjectUtils.isNotEmpty(rsArray)) {
                JSONObject rsJsob = rsArray.getJSONObject(0);
                if (i == 0) {
                    rsJsob.put("id", "kafkaSources输出-" + rsJsob.getString("id"));
                } else {
                    rsJsob.put("id", "dorisSink输入-" + rsJsob.getString("id"));
                }
                rsLists.addAll(rsArray);
            }
        }
        concatTime(rsLists);
        log.debug("rest返回信息:{}", JSON.toJSONString(rsLists));
        return rsLists;
    }

    public Object recordInOut(String applicationId, String flinkJobId) {
        List<String> vertices = vertices(applicationId, flinkJobId);
        if (ObjectUtils.isEmpty(vertices)) {
            throw new ApiException(ErrorCode.Internal.UNKNOWN_ERROR, "获取Flink监控信息失败");
        }
        String baseUrl = flinkRestPrefix + applicationId + "/jobs/" + flinkJobId + "/vertices/";
        JSONArray rsLists = new JSONArray();
        for (int i = 0; i < vertices.size(); i++) {
            StringBuilder sbtmp = new StringBuilder();
            sbtmp.append(baseUrl).append(vertices.get(i));
            if (i == 0) {
                sbtmp.append("/subtasks/metrics?get=numRecordsOut");
            } else {
                sbtmp.append("/subtasks/metrics?get=numRecordsIn");
            }
            Object rs = restUtil.getForObject(sbtmp.toString());
            JSONArray rsArray = JSON.parseArray(JSON.toJSONString(rs));
            if (ObjectUtils.isNotEmpty(rsArray)) {
                JSONObject rsJsob = rsArray.getJSONObject(0);
                if (i == 0) {
                    rsJsob.put("id", "kafkaSources输出-" + rsJsob.getString("id"));
                } else {
                    rsJsob.put("id", "dorisSink输入-" + rsJsob.getString("id"));
                }
                rsLists.addAll(rsArray);
            }
        }
        concatTime(rsLists);
        log.debug("rest返回信息:{}", JSON.toJSONString(rsLists));
        return rsLists;
    }

    public Object jobManagerMemory(String applicationId) {
        Object rs = restUtil.getForObject(flinkRestPrefix + applicationId + "/jobmanager/metrics?get=Status.JVM.Memory.NonHeap.Committed,Status.JVM.Memory.Heap.Used,Status.JVM.Memory.Heap.Committed,Status.JVM.Memory.Direct.MemoryUsed,Status.JVM.Memory.Mapped.MemoryUsed");
        JSONArray jsonArray = JSON.parseArray(JSON.toJSONString(rs));
        for (int i = 0; i < jsonArray.size(); i++) {
            JSONObject jsonObject = jsonArray.getJSONObject(i);
            String value = jsonObject.getString("value");
            if (StringUtils.isBlank(value)) {
                value = "0";
            }
            DecimalFormat df = new DecimalFormat("#.####");
            BigDecimal bg = new BigDecimal(value);
            BigDecimal val = bg.divide(new BigDecimal(1048576));
            jsonObject.put("value", new BigDecimal(df.format(val)));
        }
        concatTime(jsonArray);
        log.debug("rest返回信息:{}", jsonArray);
        return jsonArray;
    }

    public Object taskManagerMemory(String applicationId) {
        String sb = flinkRestPrefix + applicationId +
                "/taskmanagers/metrics?get=Status.JVM.Memory.NonHeap.Committed,Status.JVM.Memory.Heap.Used,Status.JVM.Memory.Heap.Committed,Status.JVM.Memory.Direct.MemoryUsed,Status.JVM.Memory.Mapped.MemoryUsed";
        Object rs = restUtil.getForObject(sb);
        //单位换算
        JSONArray jsonArray = JSON.parseArray(JSON.toJSONString(rs));
        for (int i = 0; i < jsonArray.size(); i++) {
            JSONObject jsonObject = jsonArray.getJSONObject(i);
            String value = jsonObject.getString("sum");
            if (StringUtils.isBlank(value)) {
                value = "0";
            }
            DecimalFormat df = new DecimalFormat("#.####");
            BigDecimal bg = new BigDecimal(value);
            //byte=>MB
            BigDecimal val = bg.divide(new BigDecimal(1048576));
            jsonObject.put("value", new BigDecimal(df.format(val)));
        }
        concatTime(jsonArray);
        log.debug("rest返回信息:{}", JSON.toJSONString(jsonArray));
        return jsonArray;
    }

    private List<String> vertices(String applicationId, String flinkJobId) {
        List<String> verticeIdList = new ArrayList<>();
        String sb = flinkRestPrefix + applicationId + "/jobs/" + flinkJobId;
        Object forObject = restUtil.getForObject(sb);
        log.debug("rest返回信息:{}", JSON.toJSONString(forObject));
        JSONObject flinkDetail = JSON.parseObject(JSON.toJSONString(forObject));
        JSONArray vertices = flinkDetail.getJSONArray("vertices");
        for (int i = 0; i < vertices.size(); i++) {
            JSONObject json = vertices.getJSONObject(i);
            verticeIdList.add(json.getString("id"));
        }
        return verticeIdList;
    }

    private JSONArray verticesDetail(String applicationId, String flinkJobId, String verticeId) {
        String sb = flinkRestPrefix +
                applicationId +
                "/jobs/" +
                flinkJobId +
                "/vertices/" +
                verticeId +
                "/subtasks/metrics";
        Object forObject = restUtil.getForObject(sb);
        log.debug("rest返回信息:{}", JSON.toJSONString(forObject));
        return JSON.parseArray(JSON.toJSONString(forObject));
    }

    private void concatTime(JSONArray jsonArray) {
        String time = DateUtils.getCurrentTime();
        for (int i = 0; i < jsonArray.size(); i++) {
            JSONObject jsonObject = jsonArray.getJSONObject(i);
            jsonObject.put("time", time);
        }
    }
}
