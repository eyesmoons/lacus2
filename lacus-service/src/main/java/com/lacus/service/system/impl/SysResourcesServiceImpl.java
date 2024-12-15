package com.lacus.service.system.impl;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.lacus.dao.system.entity.SysResourcesEntity;
import com.lacus.dao.system.mapper.SysResourcesMapper;
import com.lacus.enums.ResourceType;
import com.lacus.service.system.ISysResourcesService;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author casey
 */
@Service
public class SysResourcesServiceImpl extends ServiceImpl<SysResourcesMapper, SysResourcesEntity> implements ISysResourcesService {

    @Override
    public List<SysResourcesEntity> listDirectory(ResourceType type) {
        QueryWrapper<SysResourcesEntity> wrapper = new QueryWrapper<>();
        wrapper.eq("is_directory", 1);
        wrapper.eq("type", type.getCode());
        return this.list(wrapper);
    }

    @Override
    public List<SysResourcesEntity> listResource(ResourceType type, Long pid, String fileName) {
        QueryWrapper<SysResourcesEntity> wrapper = new QueryWrapper<>();
        wrapper.eq("pid", pid);
        wrapper.eq("is_directory", 0);
        wrapper.eq("type", type.getCode());
        wrapper.like("file_name", fileName);
        return this.list(wrapper);
    }
}
