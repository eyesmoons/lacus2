package com.lacus.dao.system.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import com.lacus.common.core.base.BaseEntity;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

/**
 * @author casey
 */
@Getter
@Setter
@TableName("sys_resources")
@ApiModel(value = "SysResourcesEntity对象", description = "")
public class SysResourcesEntity extends BaseEntity<SysResourcesEntity> {

    private static final long serialVersionUID = 1L;

    @ApiModelProperty("resource id")
    @TableId(value = "id", type = IdType.AUTO)
    private Integer id;

    @ApiModelProperty("parent resource id")
    @TableField("pid")
    private Integer pid;

    @ApiModelProperty("alia name")
    @TableField("name")
    private String name;

    @ApiModelProperty("file name")
    @TableField("file_name")
    private String fileName;

    @ApiModelProperty("file path")
    @TableField("file_path")
    private String filePath;

    @TableField("remark")
    private String remark;

    @ApiModelProperty("resource type: 0 FILE，1 UDF")
    @TableField("`type`")
    private Integer type;

    @ApiModelProperty("resource size")
    @TableField("size")
    private Long size;

    @TableField("is_directory")
    private Integer isDirectory;


    @Override
    public Serializable pkVal() {
        return this.id;
    }

}
