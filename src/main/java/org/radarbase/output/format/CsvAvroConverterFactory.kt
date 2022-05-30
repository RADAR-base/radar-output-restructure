package org.radarbase.output.format

import com.opencsv.CSVReader
import com.opencsv.CSVWriter
import org.apache.avro.generic.GenericRecord
import org.radarbase.output.compression.Compression
import org.radarbase.output.util.ResourceContext.Companion.resourceContext
import org.radarbase.output.util.SuspendedCloseable.Companion.useSuspended
import org.radarbase.output.util.TimeUtil.parseDate
import org.radarbase.output.util.TimeUtil.parseDateTime
import org.radarbase.output.util.TimeUtil.parseTime
import org.radarbase.output.util.TimeUtil.toDouble
import org.slf4j.LoggerFactory
import java.io.IOException
import java.io.InputStream
import java.io.Reader
import java.io.Writer
import java.nio.file.Path
import kotlin.io.path.inputStream
import kotlin.io.path.outputStream

class CsvAvroConverterFactory : RecordConverterFactory {
    override val extension: String = ".csv"

    override val formats: Collection<String> = setOf("csv")

    @Throws(IOException::class)
    override suspend fun deduplicate(
        fileName: String,
        source: Path,
        target: Path,
        compression: Compression,
        distinctFields: Set<String>,
        ignoreFields: Set<String>,
    ): Boolean {
        val (header, lineIndexes) = source.inputStream().useSuspended { input ->
            processLines(input, compression) { header, lines ->
                if (header == null) return false
                val fields = fieldIndexes(header, distinctFields, ignoreFields)
                var count = 0
                val lineMap = lines
                    .onEach { count += 1 }
                    .mapIndexed { idx, line -> Pair(ArrayWrapper(line.byIndex(fields)), idx) }
                    .toMap(HashMap())

                if (lineMap.size == count) {
                    logger.debug("File {} is already deduplicated. Skipping.", fileName)
                    return false
                }

                Pair(
                    header,
                    lineMap.values
                        .toIntArray()
                        .apply { sort() },
                )
            }
        }

        source.inputStream().useSuspended { input ->
            processLines(input, compression) { _, lines ->
                var indexIndex = 0
                writeLines(
                    target,
                    fileName,
                    compression,
                    lines = sequenceOf(header) + lines.filterIndexed { i, _ ->
                        if (indexIndex < lineIndexes.size && lineIndexes[indexIndex] == i) {
                            indexIndex += 1
                            true
                        } else false
                    },
                )
            }
        }
        return true
    }

    private suspend fun writeLines(
        target: Path,
        fileName: String,
        compression: Compression,
        lines: Sequence<Array<String>>,
    ) {
        resourceContext {
            val csvWriter = resourceChain { target.outputStream() }
                .chain { it.buffered() }
                .chain { compression.compress(fileName, it) }
                .chain { it.writer() }
                .conclude { CSVWriter(it) }

            lines.forEach {
                csvWriter.writeNext(it, false)
            }
        }
    }

    private val fieldTimeParsers = listOf(
        fieldTimeParser("value.time") { it.parseTime() },
        fieldTimeParser("key.timeStart") { it.parseTime() },
        fieldTimeParser("key.start") { it.parseTime() },
        fieldTimeParser("value.dateTime") { it.parseDateTime()?.toDouble() },
        fieldTimeParser("value.date") { it.parseDate()?.toDouble() },
        fieldTimeParser("value.timeReceived") { it.parseTime() },
        fieldTimeParser("value.timeCompleted") { it.parseTime() },
    )

    override suspend fun readTimeSeconds(
        source: InputStream,
        compression: Compression,
    ): Pair<Array<String>, List<Double>>? {
        return processLines(source, compression) { header, lines ->
            header ?: return@processLines null

            val parsers = fieldTimeParsers.mapNotNull { (key, parser) ->
                header.indexOf(key)
                    .takeIf { it >= 0 }
                    ?.let { Pair(it, parser) }
            }

            Pair(
                header,
                if (parsers.isEmpty()) emptyList() else {
                    lines.mapNotNull { line ->
                        for ((index, parser) in parsers) {
                            parser(line[index])?.let {
                                return@mapNotNull it
                            }
                        }
                        null
                    }.toList()
                },
            )
        }
    }

    override suspend fun contains(
        source: Path,
        record: GenericRecord,
        compression: Compression,
        usingFields: Set<String>,
        ignoreFields: Set<String>,
    ): Boolean = source.inputStream().use { input ->
        processLines(input, compression) { header, lines ->
            checkNotNull(header) { "Empty file found" }
            val converter = CsvAvroDataConverter(header)
            val recordValues = converter.convertRecordValues(record).toTypedArray()
            val indexes = fieldIndexes(header, usingFields, ignoreFields)

            if (indexes == null) {
                lines.any { line -> recordValues.contentEquals(line) }
            } else {
                lines.any { line -> indexes.all { i -> recordValues[i] == line[i] } }
            }
        }
    }

    @Throws(IOException::class)
    override fun converterFor(
        writer: Writer,
        record: GenericRecord,
        writeHeader: Boolean,
        reader: Reader,
    ): CsvAvroConverter = CsvAvroConverter(writer, writeHeader, reader, headerFor(record))

    override val hasHeader: Boolean = true

    companion object {
        private val logger = LoggerFactory.getLogger(CsvAvroConverterFactory::class.java)

        private fun fieldTimeParser(name: String, method: (String) -> Double?) = Pair(name, method)

        private suspend inline fun <T> processLines(
            input: InputStream,
            compression: Compression,
            process: (header: Array<String>?, lines: Sequence<Array<String>>) -> T,
        ): T = resourceContext {
            val csvReader = resourceChain { compression.decompress(input) }
                .chain { it.bufferedReader() }
                .conclude { CSVReader(it) }

            val header = csvReader.readNext()
            val lines = if (header != null) {
                generateSequence { csvReader.readNext() }
            } else emptySequence()
            process(header, lines)
        }

        private fun fieldIndexes(
            header: Array<String>,
            usingFields: Set<String>,
            ignoreFields: Set<String>,
        ): IntArray? {
            if (usingFields.isNotEmpty()) {
                val indexes = usingFields.map { f -> header.indexOf(f) }
                if (indexes.none { idx -> idx == -1 }) {
                    return indexes.toIntArray()
                }
            }
            if (ignoreFields.isNotEmpty()) {
                val ignoreIndexes = ignoreFields.mapTo(HashSet()) { f -> header.indexOf(f) }
                if (ignoreIndexes.any { idx -> idx != -1 }) {
                    return (header.indices - ignoreIndexes).toIntArray()
                }
            }
            return null
        }

        private class ArrayWrapper<T>(val values: Array<T>) {
            private val hashCode = values.contentHashCode()

            override fun equals(other: Any?): Boolean {
                if (this === other) return true
                if (javaClass != other?.javaClass) return false

                other as ArrayWrapper<*>

                return values.contentEquals(other.values)
            }

            override fun hashCode(): Int = hashCode
        }

        @Throws(IndexOutOfBoundsException::class)
        inline fun <reified T> Array<T>.byIndex(
            indexes: IntArray?,
        ): Array<T> = if (indexes == null) this else {
            Array(indexes.size) { i -> this[indexes[i]] }
        }
    }
}
