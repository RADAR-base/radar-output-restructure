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

package org.radarbase.hdfs

import java.io.IOException
import org.radarbase.hdfs.accounting.Accountant
import org.radarbase.hdfs.accounting.RemoteLockManager
import org.radarbase.hdfs.config.HdfsSettings
import org.radarbase.hdfs.config.RestructureSettings
import org.radarbase.hdfs.data.Compression
import org.radarbase.hdfs.data.FileCacheStore
import org.radarbase.hdfs.data.RecordConverterFactory
import org.radarbase.hdfs.data.StorageDriver

/** Factory for all factory classes and settings.  */
interface FileStoreFactory {
    val pathFactory: RecordPathFactory
    val storageDriver: StorageDriver
    val compression: Compression
    val recordConverter: RecordConverterFactory
    val settings: RestructureSettings
    val hdfsSettings: HdfsSettings
    val remoteLockManager: RemoteLockManager

    @Throws(IOException::class)
    fun newFileCacheStore(accountant: Accountant): FileCacheStore
}
