package com.lacus.service.system;

import com.baomidou.mybatisplus.extension.service.IService;
import com.lacus.dao.system.entity.SysResourcesEntity;
import com.lacus.enums.ResourceType;

import java.util.List;

/**
 * <p>
 *  服务类
 * </p>
 *
 * @author casey
 */
public interface ISysResourcesService extends IService<SysResourcesEntity> {

    List<SysResourcesEntity> listDirectory(ResourceType type);

    List<SysResourcesEntity> listResource(ResourceType type, Long pid, String fileName, Integer isDirectory);
}
