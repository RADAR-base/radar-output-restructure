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

package org.radarbase.hdfs.format

import java.io.BufferedOutputStream
import java.io.BufferedReader
import java.io.IOException
import java.io.InputStreamReader
import java.io.OutputStreamWriter
import java.io.Reader
import java.io.Writer
import java.nio.file.Files
import java.nio.file.Path
import java.util.LinkedHashSet
import java.util.regex.Pattern
import org.apache.avro.generic.GenericRecord
import org.radarbase.hdfs.compression.Compression

interface RecordConverterFactory : Format {
    /**
     * Create a converter to write records of given type to given writer. A header is needed only
     * in certain converters. The given record is not converted yet, it is only used as an example.
     * @param writer to write data to
     * @param record to generate the headers and schemas from.
     * @param writeHeader whether to write a header, if applicable
     * @return RecordConverter that is ready to be used
     * @throws IOException if the converter could not be created
     */
    @Throws(IOException::class)
    fun converterFor(writer: Writer, record: GenericRecord, writeHeader: Boolean, reader: Reader): RecordConverter

    val hasHeader: Boolean
        get() = false

    @Throws(IOException::class)
    fun deduplicate(fileName: String, source: Path, target: Path,
                    compression: Compression, distinctFields: List<String> = emptyList()) {
        val withHeader = hasHeader

        val (header, lines) = Files.newInputStream(source).use {
            inFile -> compression.decompress(inFile).use {
            zipIn -> InputStreamReader(zipIn).use {
            inReader -> BufferedReader(inReader).use {
            reader ->
            readFile(reader, withHeader)
        } } } }

        Files.newOutputStream(target).use {
            fileOut -> BufferedOutputStream(fileOut).use {
            bufOut -> compression.compress(fileName, bufOut).use {
            zipOut -> OutputStreamWriter(zipOut).use {
            writer ->
            writeFile(writer, header, lines)
        } } } }
    }

    override fun matchesFilename(name: String): Boolean {
        return name.matches((".*" + Pattern.quote(extension) + "(\\.[^.]+)?").toRegex())
    }

    companion object {
        /**
         * @param reader file to read from
         * @param lines lines in the file to increment to
         * @return header
         */
        @Throws(IOException::class)
        fun readFile(reader: BufferedReader, withHeader: Boolean): Pair<String?, Set<String>> {
            val header = if (withHeader) {
                reader.readLine() ?: return Pair(null, emptySet())
            } else null

            return Pair(header, generateSequence { reader.readLine() }
                    .toCollection(LinkedHashSet()))
        }

        @Throws(IOException::class)
        fun writeFile(writer: Writer, header: String?, lines: Collection<String>) {
            if (header != null) {
                writer.write(header)
                writer.write('\n'.toInt())
            }

            for (line in lines) {
                writer.write(line)
                writer.write('\n'.toInt())
            }
        }
    }
}
