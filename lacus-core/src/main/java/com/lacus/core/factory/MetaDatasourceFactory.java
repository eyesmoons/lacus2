package com.lacus.core.factory;

import com.lacus.core.processors.AbsDatasourceProcessor;
import com.lacus.core.processors.IDatasourceProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

@Component
public class MetaDatasourceFactory {
    private static final Logger logger = LoggerFactory.getLogger(MetaDatasourceFactory.class);

    private final Map<String, AbsDatasourceProcessor> context = new HashMap<>();

    /**
     * 所有processor必须注册到Factory
     */
    public void register() {
        ServiceLoader<AbsDatasourceProcessor> classList = ServiceLoader.load(AbsDatasourceProcessor.class);
        for (AbsDatasourceProcessor processor : classList) {
            try {
                register(processor);
            } catch (Exception e) {
                logger.error("new instance {} error: ", processor.getClass().getName(), e);
            }
        }
    }

    private void register(AbsDatasourceProcessor processor) {
        logger.info("数据源[{}]注册成功！", processor.getDatasourceName());
        context.put(processor.getDatasourceName(), processor);
    }

    public IDatasourceProcessor getProcessor(String name) {
        return context.get(name);
    }

    private MetaDatasourceFactory() {
    }
}