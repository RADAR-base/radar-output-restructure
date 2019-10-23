/*
 * Copyright 2018 The Hyve
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

package org.radarbase.hdfs.accounting;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.radarbase.hdfs.FileStoreFactory;
import org.radarbase.hdfs.config.RestructureSettings;
import org.radarbase.hdfs.data.StorageDriver;
import org.radarbase.hdfs.util.DirectFunctionalValue;
import org.radarbase.hdfs.util.TemporaryDirectory;
import org.radarbase.hdfs.util.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Accountant implements Flushable, Closeable {
    private static final Logger logger = LoggerFactory.getLogger(Accountant.class);

    private static final Path OFFSETS_FILE_NAME = Paths.get("offsets");
    private final OffsetRangeFile offsetFile;
    private final TemporaryDirectory tempDir;

    public Accountant(FileStoreFactory factory, String topic) throws IOException {
        StorageDriver storage = factory.getStorageDriver();
        RestructureSettings settings = factory.getSettings();

        tempDir = new TemporaryDirectory(settings.getTempDir(), "accounting-");
        Path offsetPath = settings.getOutputPath()
                .resolve(OFFSETS_FILE_NAME)
                .resolve(topic + ".csv");
        this.offsetFile = OffsetRangeFile.read(storage, offsetPath);
        this.offsetFile.setTempDir(tempDir.getPath());
    }

    public void process(Ledger ledger) {
        long timeProcess = System.nanoTime();
        offsetFile.addAll(ledger.offsets);
        offsetFile.triggerWrite();
        Timer.getInstance().add("accounting.process", timeProcess);
    }

    @Override
    public void close() throws IOException {
        long timeClose = System.nanoTime();
        IOException exception = null;

        try {
            offsetFile.close();
        } catch (IOException ex) {
            logger.error("Failed to close offsets", ex);
            exception = ex;
        }

        tempDir.close();

        if (exception != null) {
            throw exception;
        }
        Timer.getInstance().add("accounting.close", timeClose);
    }

    public OffsetRangeSet getOffsets() {
        return offsetFile.getOffsets();
    }

    @Override
    public void flush() throws IOException {
        long timeFlush = System.nanoTime();

        try {
            offsetFile.flush();
        } finally {
            Timer.getInstance().add("accounting.flush", timeFlush);
        }
    }

    public static class Ledger {
        private final OffsetRangeSet offsets;

        public Ledger() {
            offsets = new OffsetRangeSet(DirectFunctionalValue::new);
        }

        public void add(Transaction transaction) {
            long timeAdd = System.nanoTime();
            offsets.add(transaction.topicPartition, transaction.offset);
            Timer.getInstance().add("accounting.add", timeAdd);
        }
    }

    public static class Transaction {
        private final TopicPartition topicPartition;
        private final long offset;

        public Transaction(TopicPartition topicPartition, long offset) {
            this.topicPartition = topicPartition;
            this.offset = offset;
        }

        public TopicPartition getTopicPartition() {
            return topicPartition;
        }
    }
}
