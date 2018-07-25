package org.radarcns.hdfs;

import org.radarcns.hdfs.accounting.Accountant;
import org.radarcns.hdfs.config.HdfsSettings;
import org.radarcns.hdfs.config.RestructureSettings;
import org.radarcns.hdfs.data.Compression;
import org.radarcns.hdfs.data.FileCacheStore;
import org.radarcns.hdfs.data.RecordConverterFactory;
import org.radarcns.hdfs.data.StorageDriver;

import java.io.IOException;

/** Factory for all factory classes and settings. */
public interface FileStoreFactory {
    FileCacheStore newFileCacheStore(Accountant accountant) throws IOException;
    RecordPathFactory getPathFactory();
    StorageDriver getStorageDriver();
    Compression getCompression();
    RecordConverterFactory getRecordConverter();
    RestructureSettings getSettings();
    HdfsSettings getHdfsSettings();
}
