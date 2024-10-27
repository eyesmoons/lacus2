package com.lacus.domain.system.config;

import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.lacus.common.core.page.PageDTO;
import com.lacus.core.cache.CacheCenter;
import com.lacus.dao.system.entity.SysConfigEntity;
import com.lacus.domain.system.config.command.ConfigUpdateCommand;
import com.lacus.domain.system.config.dto.ConfigDTO;
import com.lacus.domain.system.config.model.ConfigModel;
import com.lacus.domain.system.config.model.ConfigModelFactory;
import com.lacus.domain.system.config.query.ConfigQuery;
import com.lacus.service.system.ISysConfigService;
import com.lacus.utils.RestUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Service
public class ConfigApplicationService {

    @Autowired
    private ISysConfigService configService;

    @Autowired
    private RestUtil restUtil;

    public PageDTO getConfigList(ConfigQuery query) {
        Page<SysConfigEntity> page = configService.page(query.toPage(), query.toQueryWrapper());
        List<ConfigDTO> records = page.getRecords().stream().map(ConfigDTO::new).collect(Collectors.toList());
        return new PageDTO(records, page.getTotal());
    }

    public ConfigDTO getConfigInfo(Long id) {
        SysConfigEntity byId = configService.getById(id);
        return new ConfigDTO(byId);
    }

    public void updateConfig(ConfigUpdateCommand updateCommand) {
        ConfigModel configModel = ConfigModelFactory.loadFromUpdateCommand(updateCommand, configService);

        configModel.checkCanBeModify();
        configModel.updateById();

        CacheCenter.configCache.get(configModel.getConfigKey());
        CacheCenter.configCache.invalidate(configModel.getConfigKey());
    }
}
