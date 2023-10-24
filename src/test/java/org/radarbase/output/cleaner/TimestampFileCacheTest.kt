package org.radarbase.output.cleaner

import kotlinx.coroutines.test.runTest
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.avro.generic.GenericRecordBuilder
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.`is`
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.io.TempDir
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock
import org.radarbase.output.FileStoreFactory
import org.radarbase.output.compression.IdentityCompression
import org.radarbase.output.config.LocalConfig
import org.radarbase.output.data.JsonAvroConverterTest.Companion.resourceStream
import org.radarbase.output.format.CsvAvroConverterFactory
import org.radarbase.output.target.LocalTargetStorage
import org.radarbase.output.util.ResourceContext.Companion.resourceContext
import java.io.ByteArrayInputStream
import java.io.FileNotFoundException
import java.nio.file.Path
import kotlin.io.path.bufferedWriter

internal class TimestampFileCacheTest {
    private lateinit var record: GenericData.Record
    private var now: Double = 0.0
    private lateinit var schema: Schema
    private lateinit var factory: FileStoreFactory
    private lateinit var csvConverter: CsvAvroConverterFactory

    @BeforeEach
    fun setUp(@TempDir dir: Path) {
        csvConverter = CsvAvroConverterFactory()
        factory = mock {
            on { recordConverter } doReturn csvConverter
            on { targetStorage } doReturn LocalTargetStorage(dir, LocalConfig())
            on { compression } doReturn IdentityCompression()
        }
        schema = Schema.Parser().parse(javaClass.resourceStream("android_phone_light.avsc"))
        now = System.currentTimeMillis() / 1000.0
        record = GenericRecordBuilder(schema)
            .set(
                "key",
                GenericRecordBuilder(schema.getField("key")!!.schema())
                    .set("projectId", "p")
                    .set("userId", "u")
                    .set("sourceId", "s")
                    .build(),
            )
            .set(
                "value",
                GenericRecordBuilder(schema.getField("value")!!.schema())
                    .set("time", now)
                    .set("timeReceived", now + 1.0)
                    .set("light", 1.0f)
                    .build(),
            )
            .build()
    }

    @Test
    fun testFileCacheFound(@TempDir path: Path) = runTest {
        val targetPath = path.resolve("test.avro")
        writeRecord(targetPath, record)
        val timestampFileCache = TimestampFileCache(factory, targetPath).apply {
            initialize()
        }
        assertThat(timestampFileCache.contains(record), `is`(true))
    }

    private suspend fun writeRecord(path: Path, record: GenericRecord) {
        resourceContext {
            val wr = this.createResource { path.bufferedWriter() }
            val emptyReader = resourceChain { ByteArrayInputStream(ByteArray(0)) }
                .conclude { it.reader() }

            csvConverter.converterFor(wr, record, true, emptyReader).use { converter ->
                converter.writeRecord(record)
            }
        }
    }

    @Test
    fun testFileCacheNotFound(@TempDir path: Path) = runTest {
        val targetPath = path.resolve("test.avro")
        assertThrows<FileNotFoundException> {
            TimestampFileCache(factory, targetPath)
                .initialize()
        }
    }

    @Test
    fun testHeaderMismatch(@TempDir path: Path) = runTest {
        val targetPath = path.resolve("test.avro")
        targetPath.bufferedWriter().use { writer ->
            writer.write("key.projectId,key.userId,key.sourceId,value.time,value.timeReceived,value.luminance")
        }
        val cache = TimestampFileCache(factory, targetPath).apply { initialize() }
        assertThrows<IllegalArgumentException> { cache.contains(record) }
    }

    @Test
    fun testNotFound(@TempDir path: Path) = runTest {
        val targetPath = path.resolve("test.avro")

        val otherRecord = GenericRecordBuilder(record)
            .set(
                "value",
                GenericRecordBuilder(record.get("value") as GenericData.Record)
                    .set("time", now + 1.0)
                    .set("timeReceived", now + 2.0)
                    .build(),
            )
            .build()

        writeRecord(targetPath, otherRecord)
        val cache = TimestampFileCache(factory, targetPath)
        cache.initialize()
        assertThat(cache.contains(record), `is`(false))
    }
}
