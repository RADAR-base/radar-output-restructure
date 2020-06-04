package org.radarbase.output.format

import com.fasterxml.jackson.databind.JsonNode
import org.apache.avro.generic.GenericRecord
import org.radarbase.output.compression.Compression
import org.radarbase.output.format.JsonAvroConverter.Companion.JSON_READER
import org.radarbase.output.format.JsonAvroConverter.Companion.JSON_WRITER
import org.radarbase.output.util.TimeUtil.getDate
import org.radarbase.output.util.TimeUtil.parseDate
import org.radarbase.output.util.TimeUtil.parseDateTime
import org.radarbase.output.util.TimeUtil.toDouble
import java.io.*
import java.nio.file.Files
import java.nio.file.Path

class JsonAvroConverterFactory : RecordConverterFactory {
    override val extension: String = ".json"

    override val formats: Collection<String> = setOf("json")

    private val converter = JsonAvroDataConverter()

    @Throws(IOException::class)
    override fun converterFor(writer: Writer,
                              record: GenericRecord,
                              writeHeader: Boolean,
                              reader: Reader): RecordConverter = JsonAvroConverter(writer, converter)

    override fun readTimeSeconds(source: InputStream, compression: Compression): Pair<Array<String>?, List<Double>>? {
        return compression.decompress(source).use {
            zipIn -> InputStreamReader(zipIn).use {
            inReader -> BufferedReader(inReader).use {
            reader ->
            Pair(null, fileSequence(reader, hasHeader)
                    .mapNotNull {
                        val record = JSON_READER.readTree(it)
                        getDate(record.get("key"), record.get("value"))
                    }
                    .toList())
        } } }
    }

    override fun contains(source: Path, record: GenericRecord, compression: Compression, usingFields: Set<String>, ignoreFields: Set<String>): Boolean {
        val recordString = JSON_WRITER.writeValueAsString(converter.convertRecord(record))

        return Files.newInputStream(source).use {
            inFile -> compression.decompress(inFile).use {
            zipIn -> InputStreamReader(zipIn).use {
            inReader -> BufferedReader(inReader).use {
            reader ->
            fileSequence(reader, hasHeader)
                    .any { recordString == it }
        } } } }
    }

    companion object {
        fun fileSequence(reader: BufferedReader, withHeader: Boolean): Sequence<String> {
            if (withHeader) reader.readLine() // skip header

            return generateSequence { reader.readLine() }
        }
    }
}
