package com.lacus.domain.system.resources.command;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

/**
 * @author shengyu
 */
@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class ResourceAddCommand {
    private Long pid;

    @NotNull(message = "资源类型不能为空")
    private Integer type;

    @NotBlank(message = "资源名称不能为空")
    private String name;

    @NotBlank(message = "资源名称不能为空")
    private String fileName;

    @NotBlank(message = "资源路径不能为空")
    private String filePath;

    @NotNull
    private Integer isDirectory;

    private String remark;
}
