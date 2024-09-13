package com.lacus.api.controller.config;


import com.lacus.common.core.base.BaseController;
import com.lacus.common.core.dto.ResponseDTO;
import com.lacus.common.core.page.PageDTO;
import com.lacus.core.annotations.AccessLog;
import com.lacus.dao.system.enums.dictionary.BusinessTypeEnum;
import com.lacus.domain.common.command.BulkOperationCommand;
import com.lacus.domain.system.env.EnvService;
import com.lacus.domain.system.env.command.EnvAddCommand;
import com.lacus.domain.system.env.command.EnvUpdateCommand;
import com.lacus.domain.system.env.dto.EnvDTO;
import com.lacus.domain.system.env.query.EnvQuery;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Positive;
import java.util.List;

/**
 * 环境管理 前端控制器
 *
 * @author casey
 */
@RestController
@RequestMapping("/system/env")
public class EnvController extends BaseController {

    @Autowired
    private EnvService envService;

    @PreAuthorize("@permission.has('system:env:list')")
    @GetMapping("/list")
    public ResponseDTO<PageDTO> list(EnvQuery query) {
        PageDTO pageDTO = envService.getEnvList(query);
        return ResponseDTO.ok(pageDTO);
    }

    @PreAuthorize("@permission.has('system:env:query')")
    @GetMapping(value = "/{envId}")
    public ResponseDTO<EnvDTO> getInfo(@PathVariable @NotNull @Positive Long envId) {
        return ResponseDTO.ok(envService.getEnvInfo(envId));
    }

    @PreAuthorize("@permission.has('system:env:add')")
    @AccessLog(title = "环境变量管理", businessType = BusinessTypeEnum.ADD)
    @PostMapping
    public ResponseDTO<?> add(@RequestBody EnvAddCommand addCommand) {
        envService.addEnv(addCommand);
        return ResponseDTO.ok();
    }

    @PreAuthorize("@permission.has('system:env:edit')")
    @AccessLog(title = "环境变量管理", businessType = BusinessTypeEnum.MODIFY)
    @PutMapping
    public ResponseDTO<?> edit(@RequestBody EnvUpdateCommand updateCommand) {
        envService.updateEnv(updateCommand);
        return ResponseDTO.ok();
    }

    @PreAuthorize("@permission.has('system:env:remove')")
    @AccessLog(title = "环境变量管理", businessType = BusinessTypeEnum.DELETE)
    @DeleteMapping("/{envIds}")
    public ResponseDTO<?> remove(@PathVariable List<Long> envIds) {
        envService.deleteEnv(new BulkOperationCommand<>(envIds));
        return ResponseDTO.ok();
    }
}

