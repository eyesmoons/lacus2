package com.lacus.domain.system.resources.command;

import lombok.Data;

/**
 * @author shengyu
 * @date 2024/12/11 22:19
 */
@Data
public class AddDirectoryCommand {
    private Long pid;
    private String name;
    private String remark;
}
