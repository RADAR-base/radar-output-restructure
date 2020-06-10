package org.radarbase.output.format

import com.opencsv.CSVReader
import com.opencsv.CSVWriter
import org.apache.avro.generic.GenericRecord
import org.radarbase.output.compression.Compression
import org.radarbase.output.util.TimeUtil.parseDate
import org.radarbase.output.util.TimeUtil.parseDateTime
import org.radarbase.output.util.TimeUtil.parseTime
import org.radarbase.output.util.TimeUtil.toDouble
import java.io.*
import java.nio.file.Files
import java.nio.file.Path

class CsvAvroConverterFactory: RecordConverterFactory {
    override val extension: String = ".csv"

    override val formats: Collection<String> = setOf("csv")

    @Throws(IOException::class)
    override fun deduplicate(fileName: String, source: Path, target: Path, compression: Compression, distinctFields: Set<String>, ignoreFields: Set<String>) {
        val (header, lines) = Files.newInputStream(source).use {
            processLines(it, compression) { header, lines ->
                Pair(header, lines.toList())
            }
        }

        if (header == null) return

        val indexes = fieldIndexes(header, distinctFields, ignoreFields)
        val distinct = lines.distinctByLast { line -> ArrayWrapper(line.byIndex(indexes)) }

        Files.newOutputStream(target).use { fileOut ->
            BufferedOutputStream(fileOut).use { bufOut ->
                compression.compress(fileName, bufOut).use { zipOut ->
                    OutputStreamWriter(zipOut).use { writer ->
                        CSVWriter(writer).use { csvWriter ->
                            csvWriter.writeNext(header, false)
                            csvWriter.writeAll(distinct, false)
                        }
                    }
                }
            }
        }
    }

    private val fieldTimeParsers = listOf(
            Pair<String, (String) -> Double?>("value.time", { it.parseTime() }),
            Pair<String, (String) -> Double?>("key.timeStart", { it.parseTime() }),
            Pair<String, (String) -> Double?>("key.start", { it.parseTime() }),
            Pair<String, (String) -> Double?>("value.dateTime", { it.parseDateTime()?.toDouble() }),
            Pair<String, (String) -> Double?>("value.date", { it.parseDate()?.toDouble() }),
            Pair<String, (String) -> Double?>("value.timeReceived", { it.parseTime() }),
            Pair<String, (String) -> Double?>("value.timeReceived", { it.parseTime() }),
            Pair<String, (String) -> Double?>("value.timeCompleted", { it.parseTime() })
    )

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
        private inline fun <T> processLines(
                input: InputStream,
                compression: Compression,
                process: (header: Array<String>?, lines: Sequence<Array<String>>) -> T
        ): T = compression.decompress(input).use { zipIn ->
            InputStreamReader(zipIn).use { inReader ->
                BufferedReader(inReader).use { bufReader ->
                    CSVReader(bufReader).use { csvReader ->
                        val header = csvReader.readNext()
                        val lines = if (header == null) emptySequence() else generateSequence { csvReader.readNext() }
                        process(header, lines)
                    }
                }
            }
        }


        /**
         * Make a copy of the current list that only contains distinct entries.
         * For duplicate entries, the last entry in the list is selected. Being distinct is
         * determined by the provided mapping.
         * @param <T> type of data.
         * @param <V> type that is mapped to for distinguishing. This type should have valid
         *            hashCode and equals implementations.
         */
        private inline fun <T, V> List<T>.distinctByLast(mapping: (T) -> V): List<T> {
            val map: MutableMap<V, Int> = HashMap()
            forEachIndexed { i, v ->
                map[mapping(v)] = i
            }
            return map.values.toIntArray()
                    .apply { sort() }
                    .map { i -> this[i] }
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
