package org.radarbase.output.format

import com.opencsv.CSVReader
import com.opencsv.CSVWriter
import org.apache.avro.generic.GenericRecord
import org.radarbase.output.compression.Compression
import org.radarbase.output.util.TimeUtil.parseDate
import org.radarbase.output.util.TimeUtil.parseDateTime
import org.radarbase.output.util.TimeUtil.parseTime
import org.radarbase.output.util.TimeUtil.toDouble
import org.slf4j.LoggerFactory
import java.io.*
import java.nio.file.Files
import java.nio.file.Path

class CsvAvroConverterFactory: RecordConverterFactory {
    override val extension: String = ".csv"

    override val formats: Collection<String> = setOf("csv")

    @Throws(IOException::class)
    override fun deduplicate(fileName: String, source: Path, target: Path, compression: Compression, distinctFields: Set<String>, ignoreFields: Set<String>): Boolean {
        val (header, lineIndexes) = Files.newInputStream(source).use { input ->
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

                Pair(header, lineMap.values
                        .toIntArray()
                        .apply { sort() })
            }
        }

        Files.newInputStream(source).use { input ->
            processLines(input, compression) { _, lines ->
                var indexIndex = 0
                writeLines(target, fileName, compression, sequenceOf(header) + lines.filterIndexed { i, _ ->
                    if (indexIndex < lineIndexes.size && lineIndexes[indexIndex] == i) {
                        indexIndex += 1
                        true
                    } else false
                })
            }
        }
        return true
    }

    private fun writeLines(target: Path, fileName: String, compression: Compression, lines: Sequence<Array<String>>) {
        Files.newOutputStream(target).use { fileOut ->
            BufferedOutputStream(fileOut).use { bufOut ->
                compression.compress(fileName, bufOut).use { zipOut ->
                    OutputStreamWriter(zipOut).use { writer ->
                        CSVWriter(writer).use { csvWriter ->
                            lines.forEach {
                                csvWriter.writeNext(it, false)
                            }
                        }
                    }
                }
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
            fieldTimeParser("value.timeReceived") { it.parseTime() },
            fieldTimeParser("value.timeCompleted") { it.parseTime() })

    override fun readTimeSeconds(source: InputStream, compression: Compression): Pair<Array<String>, List<Double>>? {
        return processLines(source, compression) { header, lines ->
            header ?: return@processLines null

            val parsers = fieldTimeParsers.mapNotNull { (key, parser) ->
                header.indexOf(key)
                        .takeIf { it >= 0 }
                        ?.let { Pair(it, parser) }
            }

            return Pair(header, if (parsers.isEmpty()) emptyList() else {
                lines.mapNotNull { line ->
                    for ((index, parser) in parsers) {
                        parser(line[index])?.let {
                            return@mapNotNull it
                        }
                    }
                    null
                }.toList()
            })
        }
    }

    override fun contains(
            source: Path,
            record: GenericRecord,
            compression: Compression,
            usingFields: Set<String>,
            ignoreFields: Set<String>
    ): Boolean = Files.newInputStream(source).use { input ->
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
    override fun converterFor(writer: Writer, record: GenericRecord,
                              writeHeader: Boolean, reader: Reader): CsvAvroConverter =
            CsvAvroConverter(writer, writeHeader, reader, headerFor(record))

    override val hasHeader: Boolean = true

    companion object {
        private val logger = LoggerFactory.getLogger(CsvAvroConverterFactory::class.java)

        private fun fieldTimeParser(name: String, method: (String) -> Double?) = Pair(name, method)

        private inline fun <T> processLines(
                input: InputStream,
                compression: Compression,
                process: (header: Array<String>?, lines: Sequence<Array<String>>) -> T
        ): T = compression.decompress(input).use { zipIn ->
            InputStreamReader(zipIn).use { inReader ->
                BufferedReader(inReader).use { bufReader ->
                    CSVReader(bufReader).use { csvReader ->
                        val header = csvReader.readNext()
                        val lines = if (header != null) {
                            generateSequence { csvReader.readNext() }
                        } else emptySequence()
                        process(header, lines)
                    }
                }
            }
        }

        private fun fieldIndexes(
                header: Array<String>,
                usingFields: Set<String>,
                ignoreFields: Set<String>
        ): IntArray? = usingFields
                .takeIf { it.isNotEmpty() }
                ?.map { f -> header.indexOf(f) }
                ?.takeIf { it.none { idx -> idx == -1 } }
                ?.toIntArray()
                ?: ignoreFields
                        .takeIf { it.isNotEmpty() }
                        ?.map { f -> header.indexOf(f) }
                        ?.takeIf { it.any { idx -> idx != -1 } }
                        ?.let { (header.indices - it).toIntArray() }

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
                indexes: IntArray?
        ): Array<T> = if (indexes == null) this else {
            Array(indexes.size) { i -> this[indexes[i]] }
        }
    }
}
