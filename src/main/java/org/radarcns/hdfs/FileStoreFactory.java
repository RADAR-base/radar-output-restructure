/*
 * Copyright 2018 The Hyve
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.radarcns.hdfs;

import java.io.IOException;
import org.radarcns.hdfs.accounting.Accountant;
import org.radarcns.hdfs.config.HdfsSettings;
import org.radarcns.hdfs.config.RestructureSettings;
import org.radarcns.hdfs.data.Compression;
import org.radarcns.hdfs.data.FileCacheStore;
import org.radarcns.hdfs.data.RecordConverterFactory;
import org.radarcns.hdfs.data.StorageDriver;

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
