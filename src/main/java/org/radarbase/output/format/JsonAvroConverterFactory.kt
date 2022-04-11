package org.radarbase.output.format

import org.apache.avro.generic.GenericRecord
import org.radarbase.output.compression.Compression
import org.radarbase.output.format.JsonAvroConverter.Companion.JSON_READER
import org.radarbase.output.format.JsonAvroConverter.Companion.JSON_WRITER
import org.radarbase.output.util.ResourceContext.Companion.resourceContext
import org.radarbase.output.util.TimeUtil.getDate
import java.io.*
import java.nio.file.Path
import kotlin.io.path.inputStream

class JsonAvroConverterFactory : RecordConverterFactory {
    override val extension: String = ".json"

    override val formats: Collection<String> = setOf("json")

    private val converter = JsonAvroDataConverter()

    @Throws(IOException::class)
    override fun converterFor(writer: Writer,
                              record: GenericRecord,
                              writeHeader: Boolean,
                              reader: Reader): RecordConverter = JsonAvroConverter(writer, converter)

    override fun readTimeSeconds(
        source: InputStream,
        compression: Compression,
    ): Pair<Array<String>?, List<Double>> = resourceContext {
        val reader = resourceChain { compression.decompress(source) }
            .chain { it.reader() }
            .conclude { it.buffered() }

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

    override fun contains(source: Path, record: GenericRecord, compression: Compression, usingFields: Set<String>, ignoreFields: Set<String>): Boolean {
        val recordString = JSON_WRITER.writeValueAsString(converter.convertRecord(record))

        return resourceContext {
            val reader = resourceChain { source.inputStream() }
                .chain { compression.decompress(it) }
                .chain { it.reader() }
                .conclude { it.buffered() }

            reader.contentLines()
                .any { recordString == it }
        }
    }

    fun BufferedReader.contentLines(): Sequence<String> = lineSequence()
        .drop(if (hasHeader) 1 else 0)

    companion object {
        fun fileSequence(
            reader: BufferedReader,
            withHeader: Boolean,
        ): Sequence<String> = reader.lineSequence()
            .drop(if (withHeader) 1 else 0)
    }
}
