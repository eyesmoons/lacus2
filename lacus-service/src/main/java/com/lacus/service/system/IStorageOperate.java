package com.lacus.service.system;

import com.lacus.dao.system.entity.StorageEntity;
import com.lacus.enums.ResUploadType;
import com.lacus.enums.ResourceType;

import java.io.IOException;
import java.util.List;

public interface IStorageOperate {

    boolean mkdir(String path) throws IOException;

    boolean exists(String fullName) throws IOException;

    boolean delete(String filePath, boolean recursive) throws IOException;

    boolean delete(String filePath, List<String> childrenPathArray, boolean recursive) throws IOException;

    boolean copy(String srcPath, String dstPath, boolean deleteSource, boolean overwrite) throws IOException;

    String getHdfsPath();

    void upload(String srcFile, String dstPath, boolean deleteSource, boolean overwrite) throws IOException;

    void download(String srcFilePath, String dstFile, boolean overwrite) throws IOException;

    List<String> vimFile(String filePath, int skipLineNums, int limit) throws IOException;

    List<StorageEntity> listFilesStatusRecursively(String path, ResourceType type);

    List<StorageEntity> listFilesStatus(String path, ResourceType type) throws Exception;

    StorageEntity getFileStatus(String path, ResourceType type) throws Exception;

}
