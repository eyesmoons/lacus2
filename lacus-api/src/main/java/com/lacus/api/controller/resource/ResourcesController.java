package com.lacus.api.controller.resource;

import com.lacus.common.core.base.BaseController;
import com.lacus.common.core.dto.ResponseDTO;
import com.lacus.common.core.page.PageDTO;
import com.lacus.domain.system.resources.ResourceService;
import com.lacus.domain.system.resources.query.ResourceQuery;
import com.lacus.enums.ResourceType;
import com.lacus.enums.Status;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
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
public class ResourcesController extends BaseController {

    @Autowired
    private ResourceService resourceService;

    @ApiOperation("创建目录")
    @PostMapping(value = "/directory")
    public ResponseDTO<?> createDirectory(@RequestParam(value = "type") ResourceType type, @RequestParam(value = "alia_name") String aliaName, @RequestParam(value = "currentDir") String currentDir) {
        resourceService.createDirectory(aliaName, type, currentDir);
        return ResponseDTO.ok();
    }

    @ApiOperation("上传文件")
    @PostMapping("/upload")
    public ResponseDTO<?> uploadResource(@RequestParam(value = "pid") Long pid, @RequestParam(value = "type") ResourceType type, @RequestParam(value = "alia_name") String aliaName, @RequestParam("file") MultipartFile file, @RequestParam(value = "currentDir") String currentDir) {
        resourceService.uploadResource(pid, aliaName, type, file, currentDir);
        return ResponseDTO.ok();
    }

    @ApiOperation("文件列表")
    @GetMapping(value = "/list")
    public ResponseDTO<?> queryResourceList(@RequestParam(value = "type") ResourceType type, @RequestParam(value = "pid") Long pid, @RequestParam(value = "fileName") String fileName) {
        return ResponseDTO.ok(resourceService.queryResourceList(type, pid, fileName));
    }

    @ApiOperation("文件列表分页")
    @GetMapping("/list/paging”)")
    public ResponseDTO<?> queryResourceListPaging(ResourceQuery query) {
        checkPageParams(query.getPageNum(), query.getPageSize());
        return ResponseDTO.ok(resourceService.queryResourceListPaging(query));
    }

    @ApiOperation("目录列表")
    @GetMapping(value = "/list/directory")
    public ResponseDTO<?> queryResourceDirectoryList(@RequestParam(value = "type") ResourceType type) {
        return ResponseDTO.ok(resourceService.queryResourceDirectoryList(type));
    }

    @ApiOperation("目录列表分页")
    @GetMapping("/list/directory/paging”)")
    public ResponseDTO<PageDTO> queryResourceDirectoryListPaging(ResourceQuery query) {
        checkPageParams(query.getPageNum(), query.getPageSize());
        return ResponseDTO.ok(resourceService.queryResourceListPaging(query));
    }

    @ApiOperation("删除文件")
    @DeleteMapping
    public ResponseDTO<?> deleteResource(@RequestParam(value = "id") long id) throws IOException {
        resourceService.deleteResource(id);
        return ResponseDTO.ok();
    }

    @ApiOperation("下载文件")
    @GetMapping("/download")
    public ResponseEntity<?> download(@RequestParam(value = "id") long id) {
        Resource file = resourceService.downloadResource(id);
        if (file == null) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(Status.RESOURCE_NOT_EXIST.getMsg());
        }
        return ResponseEntity
                .ok()
                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + file.getFilename() + "\"")
                .body(file);
    }
}
