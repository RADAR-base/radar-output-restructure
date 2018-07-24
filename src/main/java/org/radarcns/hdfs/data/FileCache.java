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

import org.apache.avro.generic.GenericRecord;
import org.radarcns.hdfs.FileStoreFactory;
import org.radarcns.hdfs.accounting.Accountant;
import org.radarcns.hdfs.util.Timer;
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
import java.util.concurrent.atomic.AtomicBoolean;

/** Keeps path handles of a path. */
public class FileCache implements Closeable, Flushable, Comparable<FileCache> {
    private static final Logger logger = LoggerFactory.getLogger(FileCache.class);

    private final Writer writer;
    private final RecordConverter recordConverter;
    private final StorageDriver storageDriver;
    private final Path path;
    private final Path tmpPath;
    private final Compression compression;
    private final RecordConverterFactory converterFactory;
    private final boolean deduplicate;
    private final Accountant.Ledger ledger;
    private final Accountant accountant;
    private long lastUse;
    private final AtomicBoolean hasError;

    /**
     * File cache of given path, using given converter factory.
     * @param path path to cache.
     * @param record example record to create converter from, this is not written to path.
     * @throws IOException if the file and/or temporary files cannot be correctly read or written to.
     */
    public FileCache(FileStoreFactory factory, Path path, GenericRecord record,
            @Nonnull Path tmpDir, Accountant accountant)
            throws IOException {
        storageDriver = factory.getStorageDriver();
        this.path = path;
        this.deduplicate = factory.getSettings().isDeduplicate();
        this.ledger = new Accountant.Ledger();
        this.compression = factory.getCompression();
        boolean fileIsNew = !storageDriver.exists(path) || storageDriver.size(path) == 0;
        this.tmpPath = Files.createTempFile(tmpDir, path.getFileName().toString(),
                ".tmp" + compression.getExtension());
        this.converterFactory = factory.getRecordConverter();
        this.accountant = accountant;

        OutputStream outStream = compression.compress(
                new BufferedOutputStream(Files.newOutputStream(tmpPath)));

        InputStream inputStream;
        if (fileIsNew) {
            inputStream = new ByteArrayInputStream(new byte[0]);
        } else {
            long timeCopy = System.nanoTime();
            inputStream = compression.decompress(storageDriver.newInputStream(path));

            if (!copy(path, outStream, compression)) {
                // restart output buffer
                outStream.close();
                // clear output file
                outStream = compression.compress(
                        new BufferedOutputStream(Files.newOutputStream(tmpPath)));
            }
            Timer.getInstance().add("write.copyOriginal", timeCopy);
        }

        this.writer = new OutputStreamWriter(outStream);

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
        hasError = new AtomicBoolean(false);
    }

    /**
     * Write a record to the cache.
     * @param record AVRO record
     * @return true or false based on {@link RecordConverter} write result
     * @throws IOException if the record cannot be used.
     */
    public boolean writeRecord(GenericRecord record, Accountant.Transaction transaction) throws IOException {
        long timeStart = System.nanoTime();
        boolean result = this.recordConverter.writeRecord(record);
        Timer.getInstance().add("write.convert", timeStart);
        lastUse = System.nanoTime();
        if (result) {
            ledger.add(transaction);
        }
        return result;
    }

    public void markError() {
        this.hasError.set(true);
    }

    @Override
    public void close() throws IOException {
        long timeClose = System.nanoTime();
        recordConverter.close();
        writer.close();

        if (!hasError.get()) {
            if (deduplicate) {
                long timeDedup = System.nanoTime();
                converterFactory.sortUnique(tmpPath, tmpPath, compression);
                Timer.getInstance().add("close.deduplicate", timeDedup);
            }

            long timeStore = System.nanoTime();
            storageDriver.store(tmpPath, path);
            Timer.getInstance().add("close.store", timeStore);

            accountant.process(ledger);
        }
        Timer.getInstance().add("close", timeClose);
    }

    @Override
    public void flush() throws IOException {
        long timeFlush = System.nanoTime();
        recordConverter.flush();
        Timer.getInstance().add("flush", timeFlush);
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

    private boolean copy(Path source, OutputStream sink, Compression compression) throws IOException {
        try (InputStream fileStream = storageDriver.newInputStream(source);
                InputStream copyStream = compression.decompress(fileStream)) {
            StorageDriver.copy(copyStream, sink);
            return true;
        } catch (IOException ex) {
            Path corruptPath = null;
            String suffix = "";
            for (int i = 0; corruptPath == null && i < 100; i++) {
                Path path = source.resolveSibling(source.getFileName() + ".corrupted" + suffix);
                if (!storageDriver.exists(path)) {
                    corruptPath = path;
                }
                suffix = "-" + i;
            }
            if (corruptPath != null) {
                logger.error("Original file {} could not be read: {}."
                        + " Moved to {}.", source, corruptPath, ex);
                storageDriver.move(source, corruptPath);
            } else {
                logger.error("Original file {} could not be read: {}."
                        + " Too many corrupt backups stored, removing file.", source, ex);
            }
            return false;
        }
    }
}
