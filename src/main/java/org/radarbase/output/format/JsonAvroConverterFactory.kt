package org.radarbase.output.format

import org.apache.avro.generic.GenericRecord
import org.radarbase.output.compression.Compression
import org.radarbase.output.format.JsonAvroConverter.Companion.JSON_READER
import org.radarbase.output.format.JsonAvroConverter.Companion.JSON_WRITER
import org.radarbase.output.util.ResourceContext.Companion.resourceContext
import org.radarbase.output.util.TimeUtil.getDate
import java.io.BufferedReader
import java.io.IOException
import java.io.InputStream
import java.io.Reader
import java.io.Writer
import java.nio.file.Path
import kotlin.io.path.inputStream

class JsonAvroConverterFactory : RecordConverterFactory {
    override val extension: String = ".json"

    override val formats: Collection<String> = setOf("json")

    private val converter = JsonAvroDataConverter(setOf())

    @Throws(IOException::class)
    override fun converterFor(
        writer: Writer,
        record: GenericRecord,
        writeHeader: Boolean,
        reader: Reader,
        excludeFields: Set<String>,
    ): RecordConverter = JsonAvroConverter(writer, excludeFields)

    override suspend fun readTimeSeconds(
        source: InputStream,
        compression: Compression,
    ): Pair<Array<String>?, List<Double>> = resourceContext {
        val reader = resourceChain { compression.decompress(source) }
            .conclude { it.bufferedReader() }

        Pair(
            null,
            reader.contentLines()
                .mapNotNull {
                    val record = JSON_READER.readTree(it)
                    getDate(record.get("key"), record.get("value"))
                }
                .toList(),
        )
    }

    override suspend fun contains(
        source: Path,
        record: GenericRecord,
        compression: Compression,
        usingFields: Set<String>,
        ignoreFields: Set<String>,
    ): Boolean {
        val recordString = JSON_WRITER.writeValueAsString(converter.convertRecord(record))

        return resourceContext {
            val reader = resourceChain { source.inputStream() }
                .chain { compression.decompress(it) }
                .conclude { it.bufferedReader() }

            reader.contentLines()
                .any { recordString == it }
        }
    }

    private fun BufferedReader.contentLines(): Sequence<String> = lineSequence()
        .drop(if (hasHeader) 1 else 0)
}
