package org.radarcns.hdfs;

import org.radarcns.hdfs.data.FileCacheStore;

import java.io.IOException;

public interface FileStoreFactory {
    FileCacheStore newFileCacheStore() throws IOException;
    RecordPathFactory getPathFactory();
}
