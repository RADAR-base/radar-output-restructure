package org.radarcns.hdfs;

import org.radarcns.hdfs.data.FileCacheStore;
import org.radarcns.hdfs.data.StorageDriver;

import java.io.IOException;

public interface FileStoreFactory {
    FileCacheStore newFileCacheStore() throws IOException;
    RecordPathFactory getPathFactory();
    StorageDriver getStorageDriver();
}
