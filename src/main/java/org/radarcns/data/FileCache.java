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

package org.radarcns.data;

import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipException;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

/** Keeps path handles of a path. */
public class FileCache implements Closeable, Flushable, Comparable<FileCache> {
    private static final Logger logger = LoggerFactory.getLogger(FileCache.class);
    private static final int BUFFER_SIZE = 8192;

    private final Writer writer;
    private final RecordConverter recordConverter;
    private final Path path;
    private final Path tmpPath;
    private long lastUse;

    /**
     * File cache of given path, using given converter factory.
     * @param converterFactory converter factory to create a converter to write files with.
     * @param path path to cache.
     * @param record example record to create converter from, this is not written to path.
     * @param gzip whether to gzip the records
     * @throws IOException if the file and/or temporary files cannot be correctly read or written to.
     */
    public FileCache(RecordConverterFactory converterFactory, Path path,
            GenericRecord record, boolean gzip, Path tmpDir) throws IOException {
        this.path = path;
        boolean fileIsNew = !Files.exists(path) || Files.size(path) == 0;
        OutputStream outFile;
        if (tmpDir == null) {
            this.tmpPath = null;
            outFile = Files.newOutputStream(path, StandardOpenOption.APPEND, StandardOpenOption.CREATE);
        } else {
            this.tmpPath = Files.createTempFile(tmpDir, path.getFileName().toString(),
                    gzip ? ".tmp.gz" : ".tmp");
            outFile = Files.newOutputStream(tmpPath);
        }

        OutputStream bufOut = new BufferedOutputStream(outFile);
        if (gzip) {
            bufOut = new GZIPOutputStream(bufOut);
        }

        InputStream inputStream;
        if (fileIsNew) {
            inputStream = new ByteArrayInputStream(new byte[0]);
        } else {
            inputStream = inputStream(new BufferedInputStream(Files.newInputStream(path)), gzip);

            if (tmpPath != null) {
                try {
                    copy(path, bufOut, gzip);
                } catch (ZipException ex) {
                    // restart output buffer
                    bufOut.close();
                    // clear output file
                    outFile = Files.newOutputStream(tmpPath);
                    bufOut = new GZIPOutputStream(new BufferedOutputStream(outFile));
                }
            }
        }

        this.writer = new OutputStreamWriter(bufOut);

        try (Reader reader = new InputStreamReader(inputStream)) {
            this.recordConverter = converterFactory.converterFor(writer, record, fileIsNew, reader);
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
     * @throws IOException if the record cannot be used.
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
        if (tmpPath != null) {
            Files.move(tmpPath, path, REPLACE_EXISTING);
        }
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

    private static void copy(Path source, OutputStream sink, boolean gzip) throws IOException {
        try (InputStream copyStream = inputStream(Files.newInputStream(source), gzip)) {
            copy(copyStream, sink);
        } catch (ZipException ex) {
            Path corruptPath = null;
            String suffix = "";
            for (int i = 0; corruptPath == null && i < 100; i++) {
                Path path = source.resolveSibling(source.getFileName() + ".corrupted" + suffix);
                if (!Files.exists(path)) {
                    corruptPath = path;
                }
                suffix = "-" + i;
            }
            if (corruptPath != null) {
                logger.error("Original file {} was corrupted: {}."
                        + " Moved to {}.", source, ex, corruptPath);
                Files.move(source, corruptPath);
            } else {
                logger.error("Original file {} was corrupted: {}."
                        + " Too many corrupt backups stored, removing file.", source, ex);
            }
            throw ex;
        }
    }

    private static InputStream inputStream(InputStream in, boolean gzip) throws IOException {
        return gzip ? new GZIPInputStream(in) : in;
    }

    /**
     * Reads all bytes from an input stream and writes them to an output stream.
     */
    private static void copy(InputStream source, OutputStream sink) throws IOException {
        byte[] buf = new byte[BUFFER_SIZE];
        int n;
        while ((n = source.read(buf)) > 0) {
            sink.write(buf, 0, n);
        }
    }
}
