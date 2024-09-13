package com.lacus.domain.system.resources.model;

import cn.hutool.core.bean.BeanUtil;
import com.lacus.dao.system.entity.SysResourcesEntity;
import com.lacus.domain.system.env.command.EnvAddCommand;
import com.lacus.domain.system.resources.command.ResourceAddCommand;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

/**
 * @author shengyu
 */
@EqualsAndHashCode(callSuper = true)
@Data
@NoArgsConstructor
public class ResourceModel extends SysResourcesEntity {

    public ResourceModel(SysResourcesEntity entity) {
        if (entity != null) {
            BeanUtil.copyProperties(entity, this);
        }
    }

    public void loadAddCommand(ResourceAddCommand command) {
        if (command != null) {
            BeanUtil.copyProperties(command, this);
        }
    }
}