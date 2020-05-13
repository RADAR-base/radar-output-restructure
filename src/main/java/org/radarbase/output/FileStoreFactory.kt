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

package org.radarbase.output

import java.io.IOException
import org.radarbase.output.accounting.Accountant
import org.radarbase.output.accounting.OffsetPersistenceFactory
import org.radarbase.output.accounting.RemoteLockManager
import org.radarbase.output.config.RestructureConfig
import org.radarbase.output.compression.Compression
import org.radarbase.output.format.RecordConverterFactory
import org.radarbase.output.kafka.KafkaStorage
import org.radarbase.output.path.RecordPathFactory
import org.radarbase.output.storage.StorageDriver
import org.radarbase.output.worker.FileCacheStore
import redis.clients.jedis.JedisPool

/** Factory for all factory classes and settings.  */
interface FileStoreFactory {
    val pathFactory: RecordPathFactory
    val storageDriver: StorageDriver
    val compression: Compression
    val recordConverter: RecordConverterFactory
    val config: RestructureConfig
    val remoteLockManager: RemoteLockManager
    val redisPool: JedisPool
    val kafkaStorage: KafkaStorage
    val offsetPersistenceFactory: OffsetPersistenceFactory

    @Throws(IOException::class)
    fun newFileCacheStore(accountant: Accountant): FileCacheStore
}
