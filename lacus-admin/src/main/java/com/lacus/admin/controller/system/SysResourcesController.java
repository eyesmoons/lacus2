package com.lacus.admin.controller.system;

import com.lacus.common.core.base.BaseController;
import com.lacus.common.core.dto.ResponseDTO;
import com.lacus.common.core.page.PageDTO;
import com.lacus.domain.system.resources.ResourceService;
import com.lacus.domain.system.resources.command.AddDirectoryCommand;
import com.lacus.domain.system.resources.query.ResourceQuery;
import com.lacus.enums.ResourceType;
import com.lacus.enums.Status;
import io.swagger.annotations.ApiOperation;
import org.apache.commons.lang3.ObjectUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;

/**
 * 资源管理
 *
 * @author casey
 */
@RestController
@RequestMapping("/system/resource")
public class SysResourcesController extends BaseController {

    @Autowired
    private ResourceService resourceService;

    @ApiOperation("创建目录")
    @PostMapping(value = "/directory/create")
    public ResponseDTO<?> createDirectory(@RequestBody AddDirectoryCommand command) {
        resourceService.createDirectory(command.getPid(), command.getName(), ResourceType.FILE, command.getRemark());
        return ResponseDTO.ok();
    }

    @ApiOperation("上传文件")
    @PostMapping("/file/upload")
    public ResponseDTO<?> uploadResource(@RequestParam(value = "pid") Long pid,
                                         @RequestParam(value = "aliasName", required = false) String aliasName,
                                         @RequestParam(value = "remark", required = false) String remark,
                                         MultipartFile file) {
        if (ObjectUtils.isEmpty(aliasName)) {
            aliasName = file.getOriginalFilename();
        }
        resourceService.uploadResource(pid, aliasName, remark, ResourceType.FILE, file);
        return ResponseDTO.ok();
    }

    @ApiOperation("文件列表")
    @GetMapping(value = "/file/list")
    public ResponseDTO<?> queryResourceList(@RequestParam(value = "pid", required = false) Long pid, @RequestParam(value = "fileName", required = false) String fileName) {
        return ResponseDTO.ok(resourceService.queryAllResources(pid));
    }

    @ApiOperation("文件列表分页")
    @GetMapping("/file/list/paging")
    public ResponseDTO<?> queryResourceListPaging(ResourceQuery query) {
        query.setIsDirectory(0);
        checkPageParams(query.getPageNum(), query.getPageSize());
        return ResponseDTO.ok(resourceService.queryAllResourcesPaging(query));
    }

    @ApiOperation("目录列表")
    @GetMapping(value = "/directory/list")
    public ResponseDTO<?> queryResourceDirectoryList() {
        return ResponseDTO.ok(resourceService.queryResourceDirectoryList(ResourceType.FILE));
    }

    @ApiOperation("目录列表分页")
    @GetMapping("/list/directory/paging)")
    public ResponseDTO<PageDTO> queryResourceDirectoryListPaging(ResourceQuery query) {
        query.setIsDirectory(0);
        checkPageParams(query.getPageNum(), query.getPageSize());
        return ResponseDTO.ok(resourceService.queryResourceListPaging(query));
    }

    @ApiOperation("删除文件")
    @DeleteMapping("/{id}")
    public ResponseDTO<?> deleteResource(@PathVariable(value = "id") long id) throws IOException {
        resourceService.deleteResource(id);
        return ResponseDTO.ok();
    }

    @ApiOperation("下载文件")
    @GetMapping("/file/download/{id}")
    public ResponseEntity<?> download(@PathVariable(value = "id") long id) {
        Resource file = resourceService.downloadResource(id);
        if (file == null) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(Status.RESOURCE_NOT_EXIST.getMsg());
        }
        return ResponseEntity
                .ok()
                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + file.getFilename() + "\"")
                .body(file);
    }

    @ApiOperation("预览文件")
    @GetMapping(value = "/view/{id}")
    public ResponseDTO<?> viewResource(@PathVariable("id") Long id) {
        return ResponseDTO.ok(resourceService.readResource(id));
    }
}
