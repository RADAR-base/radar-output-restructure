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

package org.radarcns.util;

import org.apache.avro.generic.GenericRecord;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

@FunctionalInterface
public interface RecordConverterFactory {
    /**
     * Create a converter to write records of given type to given writer. A header is needed only
     * in certain converters. The given record is not converted yet, it is only used as an example.
     * @param writer to write data to
     * @param record to generate the headers and schemas from.
     * @param writeHeader whether to write a header, if applicable
     * @return RecordConverter that is ready to be used
     * @throws IOException if the converter could not be created
     */
    RecordConverter converterFor(Writer writer, GenericRecord record, boolean writeHeader) throws IOException;

    default boolean hasHeader() {
        return false;
    }

    default void sortUnique(Path path) throws IOException {
        // read all lines into memory; assume a 100-byte line length
        List<String> sortedLines = new ArrayList<>((int)(Files.size(path) / 100));
        Path tempOut = Files.createTempFile("tempfile", ".tmp");
        String header;
        if (path.getFileName().endsWith(".gz")) {
            try (InputStream fileIn = Files.newInputStream(path);
                 GZIPInputStream gzipIn = new GZIPInputStream(fileIn);
                 Reader inReader = new InputStreamReader(gzipIn);
                 BufferedReader reader = new BufferedReader(inReader)) {
                header = readFile(reader, sortedLines, hasHeader());
            }
            try (OutputStream fileOut = Files.newOutputStream(tempOut);
                 GZIPOutputStream gzipOut = new GZIPOutputStream(fileOut);
                 Writer writer = new OutputStreamWriter(gzipOut)) {
                writeFile(writer, header, sortedLines);
            }
        } else {
            try (BufferedReader reader = Files.newBufferedReader(path)) {
                header = readFile(reader, sortedLines, hasHeader());
            }
            try (BufferedWriter writer = Files.newBufferedWriter(tempOut)) {
                writeFile(writer, header, sortedLines);
            }
        }
        Files.move(tempOut, path, REPLACE_EXISTING);
    }

    /**
     * @param reader file to read from
     * @param lines lines in the file to add to
     * @return header
     */
    static String readFile(BufferedReader reader, Collection<String> lines, boolean withHeader) throws IOException {
        String line = reader.readLine();
        String header;
        if (withHeader) {
            if (line == null) {
                throw new IOException("CSV file does not have header");
            }
            header = line;
            line = reader.readLine();
        } else {
            header = null;
        }
        while (line != null) {
            lines.add(line);
            line = reader.readLine();
        }
        return header;
    }

    static void writeFile(Writer writer, String header, List<String> lines) throws IOException {
        if (header != null) {
            writer.write(header);
            writer.write("\n");
        }
        // in a sorted collection, the duplicate lines will follow another
        // only need to keep the previous unique line in memory
        Collections.sort(lines);
        String previousLine = null;
        for (String line : lines) {
            if (line.equals(previousLine)) {
                continue;
            }
            writer.write(line);
            writer.write("\n");
            previousLine = line;
        }
    }
}
