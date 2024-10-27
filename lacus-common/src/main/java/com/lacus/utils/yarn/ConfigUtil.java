package com.lacus.utils.yarn;

import com.lacus.common.constant.Constants;
import com.lacus.utils.PropertiesUtil;
import com.lacus.utils.hdfs.HdfsUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class ConfigUtil {

    public static  void initConfig(Configuration conf,String pathPrefix, String defaultHdfs) {
        String separator = separator(pathPrefix);
        try {
            conf.addResource(new ByteArrayInputStream(HdfsUtil.readFile(pathPrefix + separator + Constants.HDFS_SITE_XML, defaultHdfs).getBytes()));
            conf.addResource(new ByteArrayInputStream(HdfsUtil.readFile( pathPrefix + separator + Constants.CORE_SITE_XML, defaultHdfs).getBytes()));
            conf.addResource(new ByteArrayInputStream(HdfsUtil.readFile( pathPrefix + separator + Constants.YARN_SITE_XML, defaultHdfs).getBytes()));
        } catch (Exception e) {
            throw new RuntimeException("初始化资源错误,检查配置文件是否存在:hdfs-site.xml/core-site.xml/yarn-site.xml");
        }
    }

    public static org.apache.flink.configuration.Configuration getFlinkConf(String pathPrefix, String defaultHdfs){
        try{
            org.apache.flink.configuration.Configuration config = new org.apache.flink.configuration.Configuration();;
            String separator = separator(pathPrefix);
            Properties properties = PropertiesUtil.loadPropertiesByStr(HdfsUtil.readFile(pathPrefix + separator + Constants.FLINK_CONF_YAML, defaultHdfs));
            for(Map.Entry<Object,Object> props : properties.entrySet()){
                config.setString(String.valueOf(props.getKey()),String.valueOf(props.getValue()));
            }
            return config;
        }catch (Exception e){
            throw new RuntimeException("初始化资源错误,检查配置文件是否存在:hdfs-site.xml/core-site.xml/yarn-site.xml");
        }
    }

    public static String separator(String path){
        String separator = File.separator;
        if(path.endsWith(separator)){
            separator = "";
        }
        return separator;
    }

    public static String getYarnQueueName(String flinkRunConfig) {
        String[] configs = trim(flinkRunConfig);
        for (String config : configs) {
            if (config.contains("-Dyarn.application.queue=")) {
                String value = config.split("=")[1];
                if (StringUtils.isEmpty(value)) {
                    return "default";
                }
                return value;
            }
        }
        return "default";
    }

    private static String[] trim(String cliConfig) {
        List<String> list = new ArrayList<>();
        String[] config = cliConfig.split(" ");
        for (String str : config) {
            if (StringUtils.isNotEmpty(str)) {
                list.add(str);
            }
        }
        return list.toArray(new String[0]);
    }
}
