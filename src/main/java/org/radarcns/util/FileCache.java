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

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.FileOutputStream;
import java.io.Flushable;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.zip.GZIPOutputStream;
import javax.annotation.Nonnull;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Keeps path handles of a path. */
public class FileCache implements Closeable, Flushable, Comparable<FileCache> {
    private static final Logger logger = LoggerFactory.getLogger(FileCache.class);

    private final Writer writer;
    private final RecordConverter recordConverter;
    private final Path path;
    private long lastUse;

    /**
     * File cache of given path, using given converter factory.
     * @param converterFactory converter factory to create a converter to write files with.
     * @param path path to cache.
     * @param record example record to create converter from, this is not written to path.
     * @param gzip whether to gzip the records
     * @throws IOException
     */
    public FileCache(RecordConverterFactory converterFactory, Path path,
            GenericRecord record, boolean gzip) throws IOException {
        this.path = path;
        boolean fileIsNew = !Files.exists(path) || Files.size(path) == 0;

        OutputStream outFile = Files.newOutputStream(path,
                StandardOpenOption.APPEND, StandardOpenOption.CREATE);
        OutputStream bufOut = new BufferedOutputStream(outFile);
        if (gzip) {
            bufOut = new GZIPOutputStream(bufOut);
        }

        this.writer = new OutputStreamWriter(bufOut);

        try {
            this.recordConverter = converterFactory.converterFor(writer, record, fileIsNew);
        } catch (IOException ex) {
            try {
                writer.close();
            } catch (IOException exClose) {
                logger.error("Failed to close writer for {}", path, ex);
            }
            throw ex;
        }
    }

    /**
     * Write a record to the cache.
     * @param record AVRO record
     * @return true or false based on {@link RecordConverter} write result
     * @throws IOException
     */
    public boolean writeRecord(GenericRecord record) throws IOException {
        boolean result = this.recordConverter.writeRecord(record);
        lastUse = System.nanoTime();
        return result;
    }

    @Override
    public void close() throws IOException {
        recordConverter.close();
        writer.close();
    }

    @Override
    public void flush() throws IOException {
        recordConverter.flush();
    }

    /**
     * Compares time that the filecaches were last used. If equal, it lexicographically compares
     * the absolute path of the path.
     * @param other FileCache to compare with.
     */
    @Override
    public int compareTo(@Nonnull FileCache other) {
        int result = Long.compare(lastUse, other.lastUse);
        if (result != 0) {
            return result;
        }
        return path.compareTo(other.path);
    }

    /** File that the cache is maintaining. */
    public Path getPath() {
        return path;
    }
}
