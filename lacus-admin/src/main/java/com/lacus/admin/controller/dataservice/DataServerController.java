package com.lacus.admin.controller.dataservice;

import com.lacus.common.core.dto.ResponseDTO;
import com.lacus.common.core.page.PageDTO;
import com.lacus.core.annotations.AccessLog;
import com.lacus.dao.system.enums.dictionary.BusinessTypeEnum;
import com.lacus.domain.dataserver.command.AddDataServerCommand;
import com.lacus.domain.dataserver.command.ParseParamCommand;
import com.lacus.domain.dataserver.dto.ParseParamsDTO;
import com.lacus.domain.dataserver.query.DataServerQuery;
import com.lacus.domain.dataserver.query.DataServerService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Api(value = "数据服务", tags = {"接口管理"})
@RestController
@RequestMapping("/dataservice/interface")
public class DataServerController {


    private final DataServerService dataServerService;

    public DataServerController(DataServerService dataServerService) {
        this.dataServerService = dataServerService;
    }

    @ApiOperation("新增数据服务接口")
    @PreAuthorize("@permission.has('dataservice:interface:add')")
    @AccessLog(title = "数据服务管理", businessType = BusinessTypeEnum.ADD)
    @PostMapping
    public ResponseDTO<?> add(@RequestBody @Validated AddDataServerCommand dataServiceCommand) {
        dataServerService.add(dataServiceCommand);
        return ResponseDTO.ok();
    }

    @ApiOperation("数据服务脚本解析参数接口")
    @PreAuthorize("@permission.has('dataservice:interface:add')")
    @AccessLog(title = "数据服务管理", businessType = BusinessTypeEnum.ADD)
    @PostMapping("/parseReqParam")
    public ResponseDTO<ParseParamsDTO> parseSQLReqParams(@RequestBody @Validated ParseParamCommand paramCommand) {
        ParseParamsDTO requestParamsList = dataServerService.parseSQLReqParams(paramCommand);
        return ResponseDTO.ok(requestParamsList);
    }

    @ApiOperation("数据服务分页接口")
    @PreAuthorize("@permission.has('dataservice:interface:list')")
    @AccessLog(title = "数据服务管理")
    @PostMapping("/pageList")
    public ResponseDTO<PageDTO> pageList(@RequestBody DataServerQuery query) {
        PageDTO pageDTO = dataServerService.pageList(query);
        return ResponseDTO.ok(pageDTO);
    }


}
