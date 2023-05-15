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

package org.radarbase.output.format

import com.opencsv.CSVReader
import com.opencsv.CSVWriter
import org.apache.avro.generic.GenericRecord
import java.io.IOException
import java.io.Reader
import java.io.Writer

/**
 * Converts deep hierarchical Avro records into flat CSV format. It uses a simple dot syntax in the
 * column names to indicate hierarchy. After the first data record is added, all following
 * records need to have exactly the same hierarchy (or at least a subset of it.)
 */
class CsvAvroConverter(
    private val writer: Writer,
    writeHeader: Boolean,
    reader: Reader,
    recordHeader: Array<String>,
    excludeFields: Set<String>,
) : RecordConverter {

    private val csvWriter = CSVWriter(writer)
    private val converter: CsvAvroDataConverter

    init {
        val (header, excludedFromHeader) = when {
            !writeHeader -> {
                val readHeader = CSVReader(reader).use {
                    requireNotNull(it.readNext()) { "No header found" }
                }
                Pair(
                    readHeader,
                    excludeFields - readHeader.toHashSet(),
                )
            }
            excludeFields.isEmpty() -> Pair(recordHeader, excludeFields)
            else -> {
                val excludedHeaderSet = recordHeader.toHashSet()
                Pair(
                    recordHeader.filter { it !in excludeFields }.toTypedArray(),
                    excludeFields.filterTo(HashSet()) { it in excludedHeaderSet },
                )
            }
        }
        if (writeHeader) {
            csvWriter.writeNext(header, false)
        }

        converter = CsvAvroDataConverter(header, excludedFromHeader)
    }

    /**
     * Write AVRO record to CSV file.
     * @param record the AVRO record to be written to CSV file
     * @return true if write was successful, false if cannot write record to the current CSV file
     * @throws IOException for other IO and Mapping errors
     */
    @Throws(IOException::class)
    override fun writeRecord(record: GenericRecord): Boolean {
        return try {
            csvWriter.writeNext(converter.convertRecordValues(record), false)
            true
        } catch (ex: IllegalArgumentException) {
            false
        } catch (ex: IndexOutOfBoundsException) {
            false
        }
    }

    override fun convertRecord(record: GenericRecord): Map<String, Any?> =
        converter.convertRecord(record)

    @Throws(IOException::class)
    override fun close() = writer.close()

    @Throws(IOException::class)
    override fun flush() = writer.flush()

    companion object {
        val factory = CsvAvroConverterFactory()
    }
}
