package com.lacus.admin.controller.metadata;

import com.lacus.common.core.dto.ResponseDTO;
import com.lacus.common.core.page.PageDTO;
import com.lacus.core.annotations.AccessLog;
import com.lacus.dao.metadata.enums.DatasourceTypeEnum;
import com.lacus.dao.system.enums.dictionary.BusinessTypeEnum;
import com.lacus.domain.metadata.datasource.DatasourceService;
import com.lacus.domain.metadata.datasource.command.AddMetaDatasourceCommand;
import com.lacus.domain.metadata.datasource.command.UpdateMetaDatasourceCommand;
import com.lacus.domain.metadata.datasource.dto.MetaDatasourceDTO;
import com.lacus.domain.metadata.datasource.model.MetaDatasourceModel;
import com.lacus.domain.metadata.datasource.query.DatasourceQuery;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import javax.validation.constraints.NotNull;
import java.util.List;

@Api(value = "数据源管理", tags = {"数据源定义"})
@RestController
@RequestMapping("/metadata/datasource")
public class DatasourceController {

    private final DatasourceService datasourceService;

    public DatasourceController(DatasourceService datasourceService) {
        this.datasourceService = datasourceService;
    }

    @ApiOperation("数据源列表")
    @PreAuthorize("@permission.has('metadata:datasource:list')")
    @GetMapping("/list")
    public ResponseDTO<List<MetaDatasourceModel>> list(
            @RequestParam(value = "datasourceName", required = false) String datasourceName,
            @RequestParam(value = "sourceType", required = false) String sourceType) {
        List<MetaDatasourceModel> list = datasourceService.getDatasourceList(datasourceName, sourceType);
        return ResponseDTO.ok(list);
    }

    @ApiOperation("数据源列表")
    @PreAuthorize("@permission.has('metadata:datasource:list')")
    @GetMapping("/pageList")
    public ResponseDTO<PageDTO> pageList(DatasourceQuery query) {
        PageDTO page = datasourceService.pageList(query);
        return ResponseDTO.ok(page);
    }

    @ApiOperation("新建数据源")
    @PreAuthorize("@permission.has('metadata:datasource:add')")
    @AccessLog(title = "数据源管理", businessType = BusinessTypeEnum.ADD)
    @PostMapping
    public ResponseDTO<?> add(@RequestBody @Validated AddMetaDatasourceCommand addCommand) {
        datasourceService.addDatasource(addCommand);
        return ResponseDTO.ok();
    }

    @ApiOperation("修改数据源")
    @PreAuthorize("@permission.has('metadata:datasource:edit')")
    @AccessLog(title = "数据源管理", businessType = BusinessTypeEnum.MODIFY)
    @PutMapping
    public ResponseDTO<?> edit(@Validated @RequestBody UpdateMetaDatasourceCommand updateCommand) {
        datasourceService.updateDatasource(updateCommand);
        return ResponseDTO.ok();
    }

    @ApiOperation("删除数据源")
    @PreAuthorize("@permission.has('metadata:datasource:remove')")
    @AccessLog(title = "数据源管理", businessType = BusinessTypeEnum.DELETE)
    @DeleteMapping("/{datasourceIds}")
    public ResponseDTO<?> remove(@PathVariable @NotNull List<Long> datasourceIds) {
        datasourceService.removeDatasource(datasourceIds);
        return ResponseDTO.ok();
    }

    @ApiOperation("数据源详情")
    @PreAuthorize("@permission.has('metadata:datasource:query')")
    @GetMapping(value = "/{datasourceId}")
    public ResponseDTO<MetaDatasourceDTO> getInfo(@PathVariable Long datasourceId) {
        MetaDatasourceDTO dto = datasourceService.getDatasourceInfo(datasourceId);
        return ResponseDTO.ok(dto);
    }

    @ApiOperation("测试数据源")
    @GetMapping(value = "/test/{datasourceId}")
    public ResponseDTO<Boolean> testConnection(@PathVariable Long datasourceId) {
        Boolean result = datasourceService.testConnection(datasourceId);
        return ResponseDTO.ok(result);
    }

    @ApiOperation("数据源类型下拉框")
    @GetMapping("/types")
    public ResponseDTO<List<String>> datasourceTypeList() {
        List<String> list = DatasourceTypeEnum.listAll();
        return ResponseDTO.ok(list);
    }

    @PreAuthorize("@permission.has('metadata:datasource:edit')")
    @AccessLog(title = "数据源管理", businessType = BusinessTypeEnum.MODIFY)
    @PutMapping("/{datasourceId}/status")
    public ResponseDTO<Boolean> changeStatus(@PathVariable Long datasourceId, @RequestBody UpdateMetaDatasourceCommand command) {
        return ResponseDTO.ok(datasourceService.changeStatus(datasourceId, command.getStatus()));
    }
}
