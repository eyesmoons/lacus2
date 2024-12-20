package com.lacus.domain.system.resources;

import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.google.common.io.Files;
import com.lacus.common.constant.Constants;
import com.lacus.common.core.page.PageDTO;
import com.lacus.common.exception.CustomException;
import com.lacus.dao.system.entity.StorageEntity;
import com.lacus.dao.system.entity.SysResourcesEntity;
import com.lacus.domain.system.resources.command.ResourceAddCommand;
import com.lacus.domain.system.resources.dto.ResourceComponent;
import com.lacus.domain.system.resources.dto.visitor.ResourceTreeVisitor;
import com.lacus.domain.system.resources.dto.visitor.Visitor;
import com.lacus.domain.system.resources.model.ResourceModel;
import com.lacus.domain.system.resources.model.ResourceModelFactory;
import com.lacus.domain.system.resources.query.ResourceQuery;
import com.lacus.enums.ResUploadType;
import com.lacus.enums.ResourceType;
import com.lacus.enums.Status;
import com.lacus.service.system.IStorageOperate;
import com.lacus.service.system.ISysResourcesService;
import com.lacus.utils.PropertyUtils;
import com.lacus.utils.RegexUtils;
import com.lacus.utils.file.FileUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.lacus.common.constant.Constants.JAR;
import static com.lacus.common.constant.Constants.MAX_FILE_SIZE;
import static com.lacus.common.constant.Constants.RESOURCE_FULL_NAME_MAX_LENGTH;
import static com.lacus.common.constant.Constants.UDF;

/**
 * @author shengyu
 * @date 2024/5/8 20:40
 */
@Slf4j
@Service
public class ResourceService {

    @Autowired
    private IStorageOperate storageOperate;

    @Autowired
    private ISysResourcesService sysResourcesService;

    public void createDirectory(Long pid, String name, ResourceType type, String remark) {
        if (FileUtil.directoryTraversal(name)) {
            log.warn("Parameter name is invalid, name:{}.", RegexUtils.escapeNRT(name));
            throw new CustomException(Status.VERIFY_PARAMETER_NAME_FAILED.getMsg(), Status.VERIFY_PARAMETER_NAME_FAILED.getCode());
        }

        SysResourcesEntity pResource = sysResourcesService.getById(pid);
        String fullName;
        if (ObjectUtils.isEmpty(pResource)) {
            fullName = "/" + name;
        } else {
            fullName = pResource.getFilePath() + File.separator + name;
        }

        try {
            if (checkResourceExists(fullName)) {
                log.error("resource directory {} exists, can't create again", fullName);
                throw new CustomException(Status.RESOURCE_EXIST.getMsg(), Status.RESOURCE_EXIST.getCode());
            }
        } catch (Exception e) {
            log.warn("Resource exists, can't create again, fullName:{}.", fullName, e);
            throw new CustomException("resource already exists, can't recreate");
        }

        // create directory in hdfs
        createDirectory(fullName, type);
        // save to db
        addResource(pid, name, fullName, remark, 1);
    }

    public void uploadResource(Long pid, String aliaName, String remark, ResourceType type, MultipartFile file) {
        verifyFile(pid, aliaName, type, file);

        // check resource name exists
        SysResourcesEntity pResource = sysResourcesService.getById(pid);
        String currDirNFileName = pResource.getFilePath() + File.separator + aliaName;

        try {
            if (checkResourceExists(currDirNFileName)) {
                log.error("resource {} has exist, can't recreate", RegexUtils.escapeNRT(aliaName));
                throw new CustomException(Status.RESOURCE_EXIST.getMsg(), Status.RESOURCE_EXIST.getCode());
            }
        } catch (Exception e) {
            throw new CustomException("resource already exists, can't recreate");
        }

        if (currDirNFileName.length() > RESOURCE_FULL_NAME_MAX_LENGTH) {
            log.error(
                    "Resource file's name is longer than max full name length, fullName:{}, " + "fullNameSize:{}, maxFullNameSize:{}",
                    RegexUtils.escapeNRT(aliaName), currDirNFileName.length(), RESOURCE_FULL_NAME_MAX_LENGTH);
            throw new CustomException(Status.RESOURCE_FULL_NAME_TOO_LONG_ERROR.getMsg(), Status.RESOURCE_FULL_NAME_TOO_LONG_ERROR.getCode());
        }

        // fail upload
        if (!upload(currDirNFileName, file, type)) {
            log.error("upload resource: {} file: {} failed.", RegexUtils.escapeNRT(aliaName), RegexUtils.escapeNRT(file.getOriginalFilename()));
            throw new CustomException(String.format("upload resource: %s file: %s failed.", aliaName, file.getOriginalFilename()));
        }

        // save to db
        addResource(pid, aliaName, currDirNFileName, remark, 0);
    }

    public List<SysResourcesEntity> queryResourceDirectoryList(ResourceType type) {
        return sysResourcesService.listDirectory(type);
    }

    public List<SysResourcesEntity> queryResourceList(ResourceType type, Long pid, String fileName, Integer isDirectory) {
        return sysResourcesService.listResource(type, pid, fileName, isDirectory);
    }

    public List<ResourceComponent> queryResourceListBak(Boolean isDirectory, ResourceType type, String fullName) {
        String defaultPath;
        List<StorageEntity> resourcesList;

        if (StringUtils.isBlank(fullName)) {
            defaultPath = storageOperate.getResDir();
            if (type.equals(ResourceType.UDF)) {
                defaultPath = storageOperate.getUdfDir();
            }
            resourcesList = storageOperate.listFilesStatusRecursively(defaultPath, defaultPath, type);
        } else {
            defaultPath = storageOperate.getResDir();
            if (type.equals(ResourceType.UDF)) {
                defaultPath = storageOperate.getUdfDir();
            }
            resourcesList = storageOperate.listFilesStatusRecursively(fullName, defaultPath, type);
        }
        Visitor resourceTreeVisitor = new ResourceTreeVisitor(resourcesList);
        return resourceTreeVisitor.visit(defaultPath).getChildren();
    }

    public PageDTO queryResourceListPaging(ResourceQuery query) {
        Page<SysResourcesEntity> page = sysResourcesService.page(query.toPage(), query.toQueryWrapper());
        return new PageDTO(page.getRecords(), page.getTotal());
    }

    public List<SysResourcesEntity> queryAllResources(Long pid) {
        List<SysResourcesEntity> allResources = new ArrayList<>();
        findAllResourcesRecursively(pid, allResources);
        return allResources;
    }

    public PageDTO queryAllResourcesPaging(ResourceQuery query) {
        List<SysResourcesEntity> allResources = new ArrayList<>();
        // 递归查询所有资源
        findAllResourcesRecursively(query.getPid(), allResources);
        // 分页处理
        int start = (query.getPageNum() - 1) * query.getPageSize();
        int end = Math.min(start + query.getPageSize(), allResources.size());
        List<SysResourcesEntity> pagedResources = allResources.subList(start, end);
        return new PageDTO(pagedResources, (long) allResources.size());
    }

    private void findAllResourcesRecursively(Long pid, List<SysResourcesEntity> allResources) {
        List<SysResourcesEntity> resources = sysResourcesService.listResource(ResourceType.FILE, pid, null, null);
        for (SysResourcesEntity resource : resources) {
            if (resource.getIsDirectory() == 1) {
                findAllResourcesRecursively(resource.getId(), allResources);
            } else if (resource.getIsDirectory() == 0) {
                SysResourcesEntity pResource = sysResourcesService.getById(resource.getPid());
                if (ObjectUtils.isNotEmpty(pResource)) {
                    resource.setPFilePath(pResource.getFilePath());
                }
                allResources.add(resource);
            }
        }
    }

    public void deleteResource(long id) throws IOException {
        String defaultPath = storageOperate.getResDir();
        StorageEntity resource;
        SysResourcesEntity byId = sysResourcesService.getById(id);
        try {
            if (ObjectUtils.isEmpty(byId)) {
                throw new CustomException(Status.RESOURCE_NOT_EXIST.getMsg());
            }
            resource = storageOperate.getFileStatus(byId.getFilePath(), defaultPath, null);
        } catch (Exception e) {
            log.error("{}, Resource id: {}", e.getMessage(), id, e);
            throw new CustomException(String.format(e.getMessage() + " resource id: %s", id));
        }

        if (resource == null) {
            log.error("Resource does not exist, resource id：{}", id);
            throw new CustomException(Status.RESOURCE_NOT_EXIST.getMsg());
        }
        List<String> allChildren = storageOperate.listFilesStatusRecursively(byId.getFilePath(), defaultPath, resource.getType()).stream().map(StorageEntity::getFullName).collect(Collectors.toList());
        // delete file on hdfs
        storageOperate.delete(byId.getFilePath(), allChildren, true);
        // delete from db
        sysResourcesService.removeById(id);
    }

    public Resource downloadResource(long id) {
        SysResourcesEntity byId = sysResourcesService.getById(id);
        if (ObjectUtils.isEmpty(byId)) {
            throw new CustomException(Status.RESOURCE_NOT_EXIST.getMsg());
        }

        String filePath = byId.getFilePath();
        if (filePath.endsWith("/")) {
            log.error("resource {} is directory，can't download", filePath);
            throw new CustomException("can't download directory");
        }

        String[] aliasArr = filePath.split("/");
        String alias = aliasArr[aliasArr.length - 1];
        String localFileName = FileUtil.getDownloadFilename(alias);
        log.info("Resource path is {}, download local filename is {}", alias, localFileName);

        try {
            storageOperate.download(filePath, localFileName, true);
            return FileUtil.file2Resource(localFileName);
        } catch (IOException e) {
            log.error("Download resource error, the path is {}, and local filename is {}, the error message is {}",
                    filePath, localFileName, e.getMessage());
            throw new CustomException("Download the resource file failed ,it may be related to your storage");
        }
    }

    private static void addResource(Long pid, String aliasName, String fullName, String remark, Integer isDirectory) {
        String fileName = new File(fullName).getName();
        ResourceAddCommand command = new ResourceAddCommand(pid, 0, aliasName, fileName, fullName, isDirectory, remark);
        ResourceModel model = ResourceModelFactory.loadFromAddCommand(command, new ResourceModel());
        model.insert();
    }

    private boolean checkResourceExists(String fullName) {
        boolean existResource = false;
        try {
            existResource = storageOperate.exists(fullName);
        } catch (IOException e) {
            log.error("error occurred when checking resource: {}", fullName, e);
        }
        return Boolean.TRUE.equals(existResource);
    }

    private void createDirectory(String fullName, ResourceType type) {
        String resourceRootPath = storageOperate.getDir(type);
        try {
            if (!storageOperate.exists(resourceRootPath)) {
                storageOperate.createTenantDirIfNotExists();
            }

            if (!storageOperate.mkdir(fullName)) {
                throw new CustomException(String.format("Create resource directory: %s failed.", fullName));
            }
        } catch (Exception e) {
            throw new CustomException(String.format("create resource directory: %s failed.", fullName));
        }
    }

    private void verifyFile(Long pid, String name, ResourceType type, MultipartFile file) {
        if (pid == 0) {
            throw new CustomException("根目录不支持上传文件，请选择其他目录！");
        }
        if (FileUtil.directoryTraversal(name)) {
            log.warn("Parameter file alias name verify failed, fileAliasName:{}.", RegexUtils.escapeNRT(name));
            throw new CustomException(Status.VERIFY_PARAMETER_NAME_FAILED.getMsg(), Status.VERIFY_PARAMETER_NAME_FAILED.getCode());
        }

        if (file != null && FileUtil.directoryTraversal(Objects.requireNonNull(file.getOriginalFilename()))) {
            log.warn("File original name verify failed, fileOriginalName:{}.", RegexUtils.escapeNRT(file.getOriginalFilename()));
            throw new CustomException(Status.VERIFY_PARAMETER_NAME_FAILED.getMsg(), Status.VERIFY_PARAMETER_NAME_FAILED.getCode());
        }

        if (file != null) {
            // file is empty
            if (file.isEmpty()) {
                log.warn("Parameter file is empty, fileOriginalName:{}.", RegexUtils.escapeNRT(file.getOriginalFilename()));
                throw new CustomException(Status.RESOURCE_FILE_IS_EMPTY.getMsg(), Status.RESOURCE_FILE_IS_EMPTY.getCode());
            }

            // file suffix
            String fileSuffix = Files.getFileExtension(file.getOriginalFilename());
            String nameSuffix = Files.getFileExtension(name);

            // determine file suffix
            if (!fileSuffix.equalsIgnoreCase(nameSuffix)) {
                // rename file suffix and original suffix must be consistent
                log.warn("Rename file suffix and original suffix must be consistent, fileOriginalName:{}.", RegexUtils.escapeNRT(file.getOriginalFilename()));
                throw new CustomException(Status.RESOURCE_SUFFIX_FORBID_CHANGE.getMsg(), Status.RESOURCE_SUFFIX_FORBID_CHANGE.getCode());
            }

            // If resource type is UDF, only jar packages are allowed to be uploaded, and the suffix must be .jar
            if (UDF.equals(type.name()) && !JAR.equalsIgnoreCase(fileSuffix)) {
                log.warn(Status.UDF_RESOURCE_SUFFIX_NOT_JAR.getMsg());
                throw new CustomException(Status.UDF_RESOURCE_SUFFIX_NOT_JAR.getMsg(), Status.UDF_RESOURCE_SUFFIX_NOT_JAR.getCode());
            }
            if (file.getSize() > MAX_FILE_SIZE) {
                log.warn("Resource file size is larger than max file size, fileOriginalName:{}, fileSize:{}, maxFileSize:{}.", RegexUtils.escapeNRT(file.getOriginalFilename()), file.getSize(), MAX_FILE_SIZE);
                throw new CustomException(Status.RESOURCE_SIZE_EXCEED_LIMIT.getMsg(), Status.RESOURCE_SIZE_EXCEED_LIMIT.getCode());
            }
        }
    }

    private boolean upload(String fullName, MultipartFile file, ResourceType type) {
        // save to local
        String fileSuffix = Files.getFileExtension(Objects.requireNonNull(file.getOriginalFilename()));
        String nameSuffix = Files.getFileExtension(fullName);

        // determine file suffix
        if (!fileSuffix.equalsIgnoreCase(nameSuffix)) {
            return false;
        }
        // random file name
        String localFilename = FileUtil.getUploadFilename(UUID.randomUUID().toString());

        // save file to hdfs, and delete original file
        String resourcePath = storageOperate.getHdfsPath() + fullName;
        try {
            // if tenant dir not exists
            if (!storageOperate.exists(resourcePath)) {
                storageOperate.createTenantDirIfNotExists();
            }
            FileUtil.copyInputStreamToFile(file, localFilename);
            storageOperate.upload(localFilename, fullName, true, true);
        } catch (Exception e) {
            FileUtil.deleteFile(localFilename);
            log.error(e.getMessage(), e);
            return false;
        }
        return true;
    }

    private List<StorageEntity> queryStorageEntityList(String fullName, ResourceType type, boolean recursive) {
        List<StorageEntity> resourcesList;
        String resourceStorageType = PropertyUtils.getString(Constants.RESOURCE_STORAGE_TYPE, ResUploadType.LOCAL.name());
        String defaultPath = storageOperate.getResDir();
        if (type.equals(ResourceType.UDF)) {
            defaultPath = storageOperate.getUdfDir();
        }

        try {
            if (StringUtils.isBlank(fullName)) {
                fullName = defaultPath;
            }
            resourcesList = recursive ? storageOperate.listFilesStatusRecursively(fullName, defaultPath, type) : storageOperate.listFilesStatus(fullName, defaultPath, type);
        } catch (Exception e) {
            log.error("{} Resource path: {}", e.getMessage(), fullName, e);
            throw new CustomException(String.format(e.getMessage() + " make sure resource path: %s exists in %s", defaultPath, resourceStorageType));
        }
        return resourcesList;
    }

    public String readResource(Long id) {
        SysResourcesEntity resource = sysResourcesService.getById(id);
        if (resource == null) {
            throw new CustomException(Status.RESOURCE_NOT_EXIST.getMsg(), Status.RESOURCE_NOT_EXIST.getCode());
        }
        String fullName = storageOperate.getHdfsPath() + resource.getFilePath();
        // check preview or not by file suffix
        String nameSuffix = Files.getFileExtension(fullName);
        String resourceViewSuffixes = FileUtil.getResourceViewSuffixes();
        if (StringUtils.isNotEmpty(resourceViewSuffixes)) {
            List<String> strList = Arrays.asList(resourceViewSuffixes.split(","));
            if (!strList.contains(nameSuffix)) {
                throw new CustomException("[" + fullName + "]不支持的文件后缀：" + nameSuffix);
            }
        }
        List<String> content;
        try {
            if (storageOperate.exists(fullName)) {
                content = storageOperate.vimFile(fullName, 0, 10000);
            } else {
                log.error("read file {} does not exist in storage", fullName);
                return null;
            }
        } catch (Exception e) {
            throw new CustomException("Resource " + fullName + " read failed");
        }
        return String.join("\n", content);
    }
}
