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

package org.radarcns.hdfs;

import com.fasterxml.jackson.databind.JsonMappingException;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.radarcns.hdfs.accounting.Accountant;
import org.radarcns.hdfs.accounting.Bin;
import org.radarcns.hdfs.accounting.OffsetRange;
import org.radarcns.hdfs.accounting.OffsetRangeSet;
import org.radarcns.hdfs.accounting.TopicPartition;
import org.radarcns.hdfs.data.FileCacheStore;
import org.radarcns.hdfs.util.ProgressBar;
import org.radarcns.hdfs.util.ReadOnlyFunctionalValue;
import org.radarcns.hdfs.util.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.radarcns.hdfs.util.ProgressBar.formatTime;

public class RadarHdfsRestructure {
    private static final Logger logger = LoggerFactory.getLogger(RadarHdfsRestructure.class);

    private static final SimpleDateFormat FILE_DATE_FORMAT = new SimpleDateFormat("yyyyMMdd_HH");
    /** Number of offsets to process in a single task. */
    private static final int BATCH_SIZE = 500_000;

    static {
        FILE_DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    private final int numThreads;
    private final Configuration conf;
    private final FileStoreFactory fileStoreFactory;
    private final RecordPathFactory pathFactory;

    private LongAdder processedFileCount;
    private LongAdder processedRecordsCount;

    public RadarHdfsRestructure(FileStoreFactory factory) {
        conf = factory.getHdfsSettings().getConfiguration();
        conf.set("fs.defaultFS", "hdfs://" + factory.getHdfsSettings().getHdfsName());
        this.numThreads = factory.getSettings().getNumThreads();
        this.fileStoreFactory = factory;
        this.pathFactory = factory.getPathFactory();
    }

    public long getProcessedFileCount() {
        return processedFileCount.sum();
    }

    public long getProcessedRecordsCount() {
        return processedRecordsCount.sum();
    }

    public void start(String directoryName) throws IOException {
        // Get files and directories
        Path path = new Path(directoryName);
        FileSystem fs = path.getFileSystem(conf);

        try (Accountant accountant = new Accountant(fileStoreFactory)) {
            logger.info("Retrieving file list from {}", path);

            Instant timeStart = Instant.now();
            // Get filenames to process
            TopicFileList topicPaths = getTopicPaths(fs, path, accountant.getOffsets());
            logger.info("Time retrieving file list: {}",
                    formatTime(Duration.between(timeStart, Instant.now())));

            processPaths(topicPaths, accountant);
        } catch (InterruptedException e) {
            logger.error("Processing interrupted");
            Thread.currentThread().interrupt();
        }
    }

    private TopicFileList getTopicPaths(FileSystem fs, Path path, OffsetRangeSet seenFiles) {
        return new TopicFileList(walk(fs, path)
                .filter(f -> f.getName().endsWith(".avro"))
                .map(f -> new TopicFile(f.getParent().getParent().getName(), f))
                .filter(f -> !seenFiles.contains(f.range))
                .collect(Collectors.toList()));
    }

    private Stream<Path> walk(FileSystem fs, Path path) {
        FileStatus[] files;
        try {
            files = fs.listStatus(path);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        return Stream.of(files).parallel()
                .flatMap(f -> {
                    if (f.isDirectory()) {
                        if (f.getPath().getName().equals("+tmp")) {
                            return Stream.empty();
                        } else {
                            return walk(fs, f.getPath());
                        }
                    } else {
                        return Stream.of(f.getPath());
                    }
                });
    }

    private void processPaths(TopicFileList topicPaths, Accountant accountant) throws InterruptedException {
        logger.info("Converting {} files with {} records",
                topicPaths.files.size(), NumberFormat.getNumberInstance().format(topicPaths.size));

        processedFileCount = new LongAdder();
        processedRecordsCount = new LongAdder();
        OffsetRangeSet seenOffsets = accountant.getOffsets()
                .withFactory(ReadOnlyFunctionalValue::new);

        ExecutorService executor = Executors.newWorkStealingPool(pathFactory.isTopicPartitioned() ? this.numThreads : 1);

        ProgressBar progressBar = new ProgressBar(topicPaths.size, 50, 100, TimeUnit.MILLISECONDS);

        // Actually process the files
        topicPaths.files.stream()
                .collect(Collectors.groupingBy(TopicFile::getTopic)).values().stream()
                .map(TopicFileList::new)
                // ensure that largest values go first on the executor queue
                .sorted(Comparator.comparingLong(TopicFileList::getSize).reversed())
                .forEach(paths -> {
                    String size = NumberFormat.getNumberInstance().format(paths.size);
                    String topic = paths.files.get(0).topic;
                    logger.info("Processing {} records for topic {}", size, topic);
                    executor.execute(() -> {
                        int batchSize = (int)(BATCH_SIZE * ThreadLocalRandom.current().nextDouble(0.75, 1.25));
                        int currentSize = 0;
                        try (FileCacheStore cache = fileStoreFactory.newFileCacheStore(accountant)) {
                            for (TopicFile file : paths.files) {
                                try {
                                    this.processFile(file, cache, progressBar, seenOffsets);
                                } catch (JsonMappingException exc) {
                                    logger.error("Cannot map values", exc);
                                }
                                processedFileCount.increment();

                                currentSize += file.size();
                                if (currentSize >= batchSize) {
                                    currentSize = 0;
                                    cache.flush();
                                }
                            }
                        } catch (IOException ex) {
                            logger.error("Failed to process file", ex);
                        } catch (IllegalStateException ex) {
                            logger.warn("Shutting down");
                        }
                    });
                });

        progressBar.update(0);

        executor.shutdown();
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
        progressBar.update(topicPaths.size);
    }

    private void processFile(TopicFile file, FileCacheStore cache,
            ProgressBar progressBar, OffsetRangeSet seenOffsets) throws IOException {
        logger.debug("Reading {}", file.path);

        // Read and parseFilename avro file
        FsInput input = new FsInput(file.path, conf);

        // processing zero-length files may trigger a stall. See:
        // https://github.com/RADAR-CNS/Restructure-HDFS-topic/issues/3
        if (input.length() == 0) {
            logger.warn("File {} has zero length, skipping.", file.path);
            return;
        }

        Timer timer = Timer.getInstance();

        long timeRead = System.nanoTime();
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(input,
                new GenericDatumReader<>());

        GenericRecord record = null;
        long offset = file.range.getOffsetFrom();
        while (dataFileReader.hasNext()) {
            record = dataFileReader.next(record);
            timer.add("read", timeRead);

            long timeAccount = System.nanoTime();
            boolean alreadyContains = seenOffsets.contains(file.range.getTopicPartition(), offset);
            timer.add("accounting.create", timeAccount);
            if (!alreadyContains) {
                // Get the fields
                this.writeRecord(file.range.getTopicPartition(), record, cache, offset, 0);
            }
            processedRecordsCount.increment();
            progressBar.update(processedRecordsCount.sum());

            offset++;
            timeRead = System.nanoTime();
        }
    }

    private void writeRecord(TopicPartition topicPartition, GenericRecord record,
            FileCacheStore cache, long offset, int suffix) throws IOException {
        RecordPathFactory.RecordOrganization metadata = pathFactory.getRecordOrganization(
                topicPartition.topic, record, suffix);

        Timer timer = Timer.getInstance();

        long timeAccount = System.nanoTime();
        String timeBin = pathFactory.getTimeBin(metadata.getTime());
        Accountant.Transaction transaction = new Accountant.Transaction(topicPartition, offset,
                new Bin(topicPartition.topic, metadata.getCategory(), timeBin));
        timer.add("accounting.create", timeAccount);

        // Write data
        long timeWrite = System.nanoTime();
        FileCacheStore.WriteResponse response = cache.writeRecord(
                metadata.getPath(), record, transaction);
        timer.add("write", timeWrite);

        if (!response.isSuccessful()) {
            // Write was unsuccessful due to different number of columns,
            // try again with new file name
            writeRecord(topicPartition, record, cache, offset, ++suffix);
        }
    }

    private static class TopicFileList {
        private final List<TopicFile> files;
        private final long size;

        public TopicFileList(List<TopicFile> files) {
            this.files = files;
            this.size = files.stream()
                    .mapToInt(TopicFile::size)
                    .sum();
        }

        public long getSize() {
            return size;
        }
    }

    private static class TopicFile {
        private final String topic;
        private final Path path;
        private final OffsetRange range;

        private TopicFile(String topic, Path path) {
            this.topic = topic;
            this.path = path;
            this.range = OffsetRange.parseFilename(path.getName());
        }

        public String getTopic() {
            return topic;
        }

        public int size() {
            return 1 + (int) (range.getOffsetTo() - range.getOffsetFrom());
        }
    }
}
