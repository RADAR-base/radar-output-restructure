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

package org.radarbase.output.accounting

import org.radarbase.output.FileStoreFactory
import org.radarbase.output.util.DirectFunctionalValue
import org.radarbase.output.util.Timer.time
import org.slf4j.LoggerFactory
import java.io.Closeable
import java.io.Flushable
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Paths
import java.time.Instant

open class Accountant @Throws(IOException::class)
constructor(factory: FileStoreFactory, topic: String) : Flushable, Closeable {
    private val offsetFile: OffsetPersistenceFactory.Writer

    val offsets: OffsetRangeSet
        get() = offsetFile.offsets

    init {
        val offsetsKey = Paths.get("offsets", "$topic.json")

        val offsetPersistence = factory.offsetPersistenceFactory
        val offsets = offsetPersistence.read(offsetsKey)
        offsetFile = offsetPersistence.writer(offsetsKey, offsets)
        readDeprecatedOffsets(factory, topic)
                ?.takeUnless { it.isEmpty }
                ?.let {
                    offsetFile.addAll(it)
                    offsetFile.triggerWrite()
                }
    }

    private fun readDeprecatedOffsets(factory: FileStoreFactory, topic: String): OffsetRangeSet? {
        val offsetsPath = factory.config.paths.output
                .resolve(OFFSETS_FILE_NAME)
                .resolve("$topic.csv")

        return if (Files.exists(offsetsPath)) {
            OffsetFilePersistence(factory.storageDriver).read(offsetsPath)
                    .also { Files.delete(offsetsPath) }
        } else null
    }

    open fun process(ledger: Ledger) = time("accounting.process") {
        offsetFile.addAll(ledger.offsets)
        offsetFile.triggerWrite()
    }

    @Throws(IOException::class)
    override fun close() = time("accounting.close") {
        var exception: IOException? = null

        try {
            offsetFile.close()
        } catch (ex: IOException) {
            logger.error("Failed to close offsets", ex)
            exception = ex
        }

        if (exception != null) {
            throw exception
        }
    }

    @Throws(IOException::class)
    override fun flush() = time("accounting.flush") {
        offsetFile.flush()
    }

    class Ledger {
        internal val offsets: OffsetRangeSet = OffsetRangeSet { DirectFunctionalValue(it) }

        fun add(transaction: Transaction) = time("accounting.add") {
            offsets.add(transaction.topicPartition, transaction.offset, transaction.lastModified)
        }
    }

    class Transaction(val topicPartition: TopicPartition, internal var offset: Long, internal val lastModified: Instant)

    companion object {
        private val logger = LoggerFactory.getLogger(Accountant::class.java)
        private val OFFSETS_FILE_NAME = Paths.get("offsets")
    }
}
