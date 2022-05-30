/*
 * Copyright 2017 The Hyve
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

package org.radarbase.output.worker

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.apache.avro.generic.GenericRecord
import org.radarbase.output.FileStoreFactory
import org.radarbase.output.accounting.Accountant
import org.radarbase.output.compression.Compression
import org.radarbase.output.config.DeduplicationConfig
import org.radarbase.output.format.RecordConverter
import org.radarbase.output.format.RecordConverterFactory
import org.radarbase.output.target.TargetStorage
import org.radarbase.output.util.SuspendedCloseable
import org.radarbase.output.util.SuspendedCloseable.Companion.useSuspended
import org.radarbase.output.util.Timer.time
import org.slf4j.LoggerFactory
import java.io.*
import java.nio.file.AtomicMoveNotSupportedException
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.io.path.createTempFile
import kotlin.io.path.moveTo
import kotlin.io.path.outputStream

/** Keeps path handles of a path.  */
class FileCache(
    factory: FileStoreFactory,
    topic: String,
    /** File that the cache is maintaining.  */
    val path: Path,
    /** Local temporary directory to store files in. */
    tmpDir: Path,
    private val accountant: Accountant,
) : SuspendedCloseable, Comparable<FileCache> {

    private lateinit var writer: Writer
    private lateinit var recordConverter: RecordConverter
    private val targetStorage: TargetStorage = factory.targetStorage
    private val tmpPath: Path
    private val compression: Compression = factory.compression
    private val converterFactory: RecordConverterFactory = factory.recordConverter
    private val ledger: Accountant.Ledger = Accountant.Ledger()
    private val fileName: String = path.fileName.toString()
    private var lastUse: Long = 0
    private val hasError: AtomicBoolean = AtomicBoolean(false)
    private val deduplicate: DeduplicationConfig

    init {
        val topicConfig = factory.config.topics[topic]
        val defaultDeduplicate = factory.config.format.deduplication
        deduplicate = topicConfig?.deduplication(defaultDeduplicate) ?: defaultDeduplicate

        this.tmpPath = createTempFile(tmpDir, fileName, ".tmp" + compression.extension)
    }

    suspend fun initialize(record: GenericRecord) {
        val fileIsNew = targetStorage.status(path)?.takeIf { it.size > 0L } == null

        var outStream = compression.compress(fileName, tmpPath.outputStream().buffered())

        val inputStream: InputStream
        if (fileIsNew) {
            inputStream = ByteArrayInputStream(ByteArray(0))
        } else {
            inputStream = time("write.copyOriginal") {
                if (!copy(path, outStream, compression)) {
                    // restart output buffer
                    outStream.close()
                    // clear output file
                    outStream = compression.compress(fileName, tmpPath.outputStream().buffered())
                }
                compression.decompress(targetStorage.newInputStream(path))
            }
        }

        this.writer = outStream.writer()

        this.recordConverter = try {
            inputStream.reader().useSuspended { reader ->
                converterFactory.converterFor(writer, record, fileIsNew, reader)
            }
        } catch (ex: IOException) {
            coroutineScope {
                launch(Dispatchers.IO) {
                    try {
                        writer.close()
                    } catch (exClose: IOException) {
                        logger.error("Failed to close writer for {}", path, ex)
                    }
                }
            }

            throw ex
        }
    }

    /**
     * Write a record to the cache.
     * @param record AVRO record
     * @return true or false based on [RecordConverter] write result
     * @throws IOException if the record cannot be used.
     */
    @Throws(IOException::class)
    fun writeRecord(record: GenericRecord, transaction: Accountant.Transaction): Boolean {
        val result = time("write.convert") { this.recordConverter.writeRecord(record) }
        lastUse = System.nanoTime()
        if (result) {
            ledger.add(transaction)
        }
        return result
    }

    fun markError() {
        this.hasError.set(true)
    }

    @Throws(IOException::class)
    override suspend fun closeAndJoin() = time("close") {
        recordConverter.close()
        writer.close()

        if (!hasError.get()) {
            if (deduplicate.enable == true) {
                time("close.deduplicate") {
                    val dedupTmp = tmpPath.resolveSibling("${tmpPath.fileName}.dedup")
                    if (
                        converterFactory.deduplicate(
                            fileName = fileName,
                            source = tmpPath,
                            target = dedupTmp,
                            compression = compression,
                            distinctFields = deduplicate.distinctFields ?: emptySet(),
                            ignoreFields = deduplicate.ignoreFields ?: emptySet(),
                        )
                    ) {
                        withContext(Dispatchers.IO) {
                            try {
                                dedupTmp.moveTo(
                                    tmpPath,
                                    StandardCopyOption.REPLACE_EXISTING,
                                    StandardCopyOption.ATOMIC_MOVE,
                                )
                            } catch (ex: AtomicMoveNotSupportedException) {
                                dedupTmp.moveTo(tmpPath, StandardCopyOption.REPLACE_EXISTING)
                            }
                        }
                    }
                }
            }

            time("close.store") {
                targetStorage.store(tmpPath, path)
            }

            accountant.process(ledger)
        }
    }

    /**
     * Compares time that the filecaches were last used. If equal, it lexicographically compares
     * the absolute path of the path.
     * @param other FileCache to compare with.
     */
    override fun compareTo(other: FileCache): Int = comparator.compare(this, other)

    @Throws(IOException::class)
    private suspend fun copy(source: Path, sink: OutputStream, compression: Compression): Boolean {
        return try {
            targetStorage.newInputStream(source).use { fileStream ->
                compression.decompress(fileStream).use { copyStream ->
                    copyStream.copyTo(sink, bufferSize = 8192)
                    true
                }
            }
        } catch (ex: IOException) {
            var corruptPath: Path? = null
            var suffix = ""
            var i = 0
            while (corruptPath == null && i < 100) {
                val path = source.resolveSibling(source.fileName.toString() + ".corrupted" + suffix)
                if (targetStorage.status(path) == null) {
                    corruptPath = path
                }
                suffix = "-$i"
                i++
            }
            if (corruptPath != null) {
                logger.error(
                    "Original file {} could not be read: {}." + " Moved to {}.",
                    source,
                    ex,
                    corruptPath,
                )
                targetStorage.move(source, corruptPath)
            } else {
                logger.error(
                    "Original file {} could not be read: {}." + " Too many corrupt backups stored, removing file.",
                    source,
                    ex,
                )
            }
            false
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(FileCache::class.java)
        val comparator = compareBy(FileCache::lastUse, FileCache::path)
    }
}
