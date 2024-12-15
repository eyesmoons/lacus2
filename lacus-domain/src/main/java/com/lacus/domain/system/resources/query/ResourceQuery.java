package com.lacus.domain.system.resources.query;

import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.lacus.dao.system.entity.SysEnvEntity;
import com.lacus.dao.system.entity.SysResourcesEntity;
import com.lacus.dao.system.query.AbstractPageQuery;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.commons.lang3.ObjectUtils;

import java.util.Objects;

/**
 * @author shengyu
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class ResourceQuery extends AbstractPageQuery {
    private String fileName;
    private Long pid;
    private Integer isDirectory;

    @Override
    public QueryWrapper toQueryWrapper() {
        QueryWrapper<SysResourcesEntity> wrapper = new QueryWrapper<>();
        wrapper.in(Objects.nonNull(pid), "pid", pid);
        wrapper.eq(ObjectUtils.isNotEmpty(isDirectory), "is_directory", isDirectory);
        wrapper.like(StrUtil.isNotEmpty(fileName), "file_name", fileName);
        return wrapper;
    }
}
