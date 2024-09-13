package com.lacus.service.system;

import com.lacus.common.constant.Constants;
import com.lacus.dao.system.entity.StorageEntity;
import com.lacus.enums.ResUploadType;
import com.lacus.enums.ResourceType;
import com.lacus.utils.PropertyUtils;

import java.io.IOException;
import java.util.List;

import static com.lacus.common.constant.Constants.RESOURCE_TYPE_FILE;

public interface IStorageOperate {

    String RESOURCE_UPLOAD_PATH = PropertyUtils.getString(Constants.RESOURCE_UPLOAD_PATH, "/lacus");

    void createTenantDirIfNotExists() throws Exception;

    String getResDir();

    String getUdfDir();

    boolean mkdir(String path) throws IOException;

    String getResourceFullName(String fileName);

    default String getResourceFileName(String fullName) {
        String resDir = getResDir();
        String filenameReplaceResDir = fullName.replaceFirst(resDir, "");
        if (!filenameReplaceResDir.equals(fullName)) {
            return filenameReplaceResDir;
        }

        // Replace resource dir not effective in case of run workflow with different tenant from resource file's.
        // this is backup solution to get related path, by split with RESOURCE_TYPE_FILE
        return filenameReplaceResDir.contains(RESOURCE_TYPE_FILE)
                ? filenameReplaceResDir.split(String.format("%s/", RESOURCE_TYPE_FILE))[1]
                : filenameReplaceResDir;
    }

    String getFileName(ResourceType resourceType, String fileName);

    boolean exists(String fullName) throws IOException;

    boolean delete(String filePath, boolean recursive) throws IOException;

    boolean delete(String filePath, List<String> childrenPathArray, boolean recursive) throws IOException;

    boolean copy(String srcPath, String dstPath, boolean deleteSource, boolean overwrite) throws IOException;

    String getDir(ResourceType resourceType);

    void upload(String srcFile, String dstPath, boolean deleteSource, boolean overwrite) throws IOException;

    void download(String srcFilePath, String dstFile, boolean overwrite) throws IOException;

    List<String> vimFile(String filePath, int skipLineNums, int limit) throws IOException;

    ResUploadType returnStorageType();

    List<StorageEntity> listFilesStatusRecursively(String path, String defaultPath, ResourceType type);

    List<StorageEntity> listFilesStatus(String path, String defaultPath, ResourceType type) throws Exception;

    StorageEntity getFileStatus(String path, String defaultPath, ResourceType type) throws Exception;
}
