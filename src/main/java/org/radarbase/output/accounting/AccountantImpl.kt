package org.radarbase.output.accounting

import kotlinx.coroutines.CoroutineScope
import org.radarbase.output.FileStoreFactory
import org.radarbase.output.util.Timer
import org.slf4j.LoggerFactory
import java.io.IOException
import java.nio.file.Paths

open class AccountantImpl(
    private val factory: FileStoreFactory,
    private val topic: String,
) : Accountant {
    private lateinit var offsetFile: OffsetPersistenceFactory.Writer

    override val offsets: OffsetRangeSet
        get() = offsetFile.offsets

    override suspend fun initialize(scope: CoroutineScope) {
        val offsetsKey = Paths.get("offsets", "$topic.json")

        val offsetPersistence = factory.offsetPersistenceFactory

        val offsets = offsetPersistence.read(offsetsKey)
        offsetFile = offsetPersistence.writer(scope, offsetsKey, offsets)
    }

    override suspend fun remove(range: TopicPartitionOffsetRange) =
        Timer.time("accounting.remove") {
            offsetFile.offsets.remove(range)
            offsetFile.triggerWrite()
        }

    override suspend fun process(ledger: Accountant.Ledger) = Timer.time("accounting.process") {
        offsetFile.addAll(ledger.offsets)
        offsetFile.triggerWrite()
    }

    @Throws(IOException::class)
    override suspend fun closeAndJoin() = Timer.time("accounting.close") {
        var exception: IOException? = null

        try {
            offsetFile.closeAndJoin()
        } catch (ex: IOException) {
            logger.error("Failed to close offsets", ex)
            exception = ex
        }

        if (exception != null) {
            throw exception
        }
    }

    override suspend fun flush() = Timer.time("accounting.flush") {
        offsetFile.flush()
    }

    companion object {
        private val logger = LoggerFactory.getLogger(Accountant::class.java)
        private val OFFSETS_FILE_NAME = Paths.get("offsets")
    }
}
