package com.lacus.api.controller.datasync;

import com.lacus.common.core.dto.ResponseDTO;
import com.lacus.common.core.page.PageDTO;
import com.lacus.domain.datasync.instance.JobInstanceService;
import com.lacus.domain.datasync.instance.query.JobInstancePageQuery;
import io.swagger.annotations.Api;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @created by shengyu on 2024/2/29 21:04
 */
@Api(value = "任务实例相关接口", tags = {"任务实例相关接口"})
@RestController
@RequestMapping("/datasync/job/instance")
public class JobInstanceController {

    @Autowired
    private JobInstanceService instanceService;

    @GetMapping("pageList")
    @PreAuthorize("@permission.has('datasync:instance:list')")
    public ResponseDTO<PageDTO> pageList(JobInstancePageQuery query) {
        PageDTO page = instanceService.pageList(query);
        return ResponseDTO.ok(page);
    }
}