package com.lacus.admin.controller.datasync;

import com.lacus.common.core.dto.ResponseDTO;
import com.lacus.common.core.page.PageDTO;
import com.lacus.core.annotations.AccessLog;
import com.lacus.dao.datasync.entity.DataSyncJobCatalogEntity;
import com.lacus.dao.system.enums.dictionary.BusinessTypeEnum;
import com.lacus.domain.datasync.jobCatalog.JobCatalogService;
import com.lacus.domain.datasync.jobCatalog.command.AddJobCatalogCommand;
import com.lacus.domain.datasync.jobCatalog.command.UpdateJobCatalogCommand;
import com.lacus.domain.datasync.jobCatalog.query.JobCatalogQuery;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.validation.constraints.NotNull;
import java.util.List;

@Api(value = "任务分组相关接口", tags = {"任务分组相关接口"})
@RestController
@RequestMapping("/datasync/job/catalog")
public class JobCatalogController {

    @Autowired
    private JobCatalogService jobCatalogService;

    @ApiOperation("任务分组列表")
    @PreAuthorize("@permission.has('datasync:catalog:list')")
    @GetMapping("/list")
    public ResponseDTO<List<DataSyncJobCatalogEntity>> list(@RequestParam(value = "catalogName", required = false) String catalogName) {
        List<DataSyncJobCatalogEntity> list = jobCatalogService.list(catalogName);
        return ResponseDTO.ok(list);
    }

    @ApiOperation("任务分组分页列表")
    @PreAuthorize("@permission.has('datasync:catalog:list')")
    @GetMapping("/pageList")
    public ResponseDTO<PageDTO> pageList(JobCatalogQuery query) {
        PageDTO page = jobCatalogService.pageList(query);
        return ResponseDTO.ok(page);
    }

    @ApiOperation("任务分组详情")
    @PreAuthorize("@permission.has('datasync:catalog:list')")
    @GetMapping("/{catalogId}")
    public ResponseDTO<DataSyncJobCatalogEntity> detail(@PathVariable("catalogId") String catalogId) {
        DataSyncJobCatalogEntity entity = jobCatalogService.detail(catalogId);
        return ResponseDTO.ok(entity);
    }

    @ApiOperation("新建任务分组")
    @PreAuthorize("@permission.has('datasync:catalog:add')")
    @AccessLog(title = "任务分组管理", businessType = BusinessTypeEnum.ADD)
    @PostMapping
    public ResponseDTO<?> add(@RequestBody @Validated AddJobCatalogCommand addCommand) {
        jobCatalogService.addJobCatalog(addCommand);
        return ResponseDTO.ok();
    }

    @ApiOperation("修改任务分组")
    @PreAuthorize("@permission.has('datasync:catalog:edit')")
    @AccessLog(title = "任务分组管理", businessType = BusinessTypeEnum.MODIFY)
    @PutMapping
    public ResponseDTO<?> edit(@Validated @RequestBody UpdateJobCatalogCommand updateCommand) {
        jobCatalogService.updateJobCatalog(updateCommand);
        return ResponseDTO.ok();
    }

    @ApiOperation("删除任务分组")
    @PreAuthorize("@permission.has('datasync:catalog:remove')")
    @AccessLog(title = "任务分组管理", businessType = BusinessTypeEnum.DELETE)
    @DeleteMapping("/{catalogIds}")
    public ResponseDTO<?> remove(@PathVariable @NotNull List<String> catalogIds) {
        jobCatalogService.removeJobCatalog(catalogIds);
        return ResponseDTO.ok();
    }
}
