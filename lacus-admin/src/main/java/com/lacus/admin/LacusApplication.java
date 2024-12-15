package com.lacus.admin;

import cn.hutool.core.date.DateUtil;
import com.lacus.core.factory.MetaDatasourceFactory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.event.EventListener;

@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class})
@ComponentScan(basePackages = "com.lacus.*")
@Slf4j
public class LacusApplication {

    @Autowired
    private MetaDatasourceFactory metaDatasourceFactory;

    public static void main(String[] args) {
        SpringApplication.run(LacusApplication.class, args);
        System.out.println("Lacus大数据平台启动成功：" + DateUtil.now());
    }

    @EventListener
    public void run(ApplicationReadyEvent event) {
        log.info("开始注册数据源...");
        metaDatasourceFactory.register();
    }
}
