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

package org.radarcns.hdfs.data;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.regex.Pattern;
import org.apache.avro.generic.GenericRecord;

public interface RecordConverterFactory extends Format {
    /**
     * Create a converter to write records of given type to given writer. A header is needed only
     * in certain converters. The given record is not converted yet, it is only used as an example.
     * @param writer to write data to
     * @param record to generate the headers and schemas from.
     * @param writeHeader whether to write a header, if applicable
     * @return RecordConverter that is ready to be used
     * @throws IOException if the converter could not be created
     */
    RecordConverter converterFor(Writer writer, GenericRecord record, boolean writeHeader, Reader reader) throws IOException;

    default boolean hasHeader() {
        return false;
    }

    default void sortUnique(Path source, Path target, Compression compression)
            throws IOException {
        // read all lines into memory; assume a 100-byte line length
        LinkedHashSet<String> sortedLines = new LinkedHashSet<>((int)(Files.size(source) / 100));
        String header;
        boolean withHeader = hasHeader();

        try (InputStream inFile = Files.newInputStream(source);
             InputStream zipIn = compression.decompress(inFile);
             Reader inReader = new InputStreamReader(zipIn);
             BufferedReader reader = new BufferedReader(inReader)) {
            header = readFile(reader, sortedLines, withHeader);
        }
        try (OutputStream fileOut = Files.newOutputStream(target);
             OutputStream bufOut = new BufferedOutputStream(fileOut);
             OutputStream zipOut = compression.compress(bufOut);
             Writer writer = new OutputStreamWriter(zipOut)) {
            writeFile(writer, header, sortedLines);
        }
    }

    /**
     * @param reader file to read from
     * @param lines lines in the file to increment to
     * @return header
     */
    static String readFile(BufferedReader reader, Collection<String> lines, boolean withHeader) throws IOException {
        String line = reader.readLine();
        String header;
        if (withHeader) {
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

    static void writeFile(Writer writer, String header, Collection<String> lines) throws IOException {
        if (header != null) {
            writer.write(header);
            writer.write('\n');
        }

        for (String line : lines) {
            writer.write(line);
            writer.write('\n');
        }
    }

    default boolean matchesFilename(String name) {
        return name.matches(".*" + Pattern.quote(getExtension()) + "(\\.[^.]+)?");
    }
}
