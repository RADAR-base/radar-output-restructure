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

package org.radarbase.hdfs;

import com.fasterxml.jackson.databind.JsonMappingException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.text.NumberFormat;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.radarbase.hdfs.accounting.Accountant;
import org.radarbase.hdfs.accounting.OffsetRange;
import org.radarbase.hdfs.accounting.OffsetRangeSet;
import org.radarbase.hdfs.accounting.RemoteLockManager;
import org.radarbase.hdfs.accounting.RemoteLockManager.RemoteLock;
import org.radarbase.hdfs.accounting.TopicPartition;
import org.radarbase.hdfs.data.FileCacheStore;
import org.radarbase.hdfs.util.ProgressBar;
import org.radarbase.hdfs.util.ReadOnlyFunctionalValue;
import org.radarbase.hdfs.util.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RadarHdfsRestructure {
    private static final Logger logger = LoggerFactory.getLogger(RadarHdfsRestructure.class);

    /** Number of offsets to process in a single task. */
    private static final long BATCH_SIZE = 500_000;

    private final int numThreads;
    private final Configuration conf;
    private final FileStoreFactory fileStoreFactory;
    private final RecordPathFactory pathFactory;
    private final long maxFilesPerTopic;
    private List<String> excludeTopics;

    private LongAdder processedFileCount;
    private LongAdder processedRecordsCount;

    private RemoteLockManager lockManager;

    public RadarHdfsRestructure(FileStoreFactory factory) {
        conf = factory.getHdfsSettings().getConfiguration();
        this.numThreads = factory.getSettings().getNumThreads();
        long maxFiles = factory.getSettings().getMaxFilesPerTopic();
        if (maxFiles < 1) {
            maxFiles = Long.MAX_VALUE;
        }
        this.maxFilesPerTopic = maxFiles;
        this.excludeTopics = factory.getSettings().getExcludeTopics();
        this.fileStoreFactory = factory;
        this.pathFactory = factory.getPathFactory();
        this.lockManager = factory.getRemoteLockManager();
    }

    public long getProcessedFileCount() {
        return processedFileCount.sum();
    }

    public long getProcessedRecordsCount() {
        return processedRecordsCount.sum();
    }

    public void start(String directoryName) throws IOException, InterruptedException {
        // Get files and directories
        Path path = new Path(directoryName);
        FileSystem fs = path.getFileSystem(conf);
        Path absolutePath = fs.getFileStatus(path).getPath();  // get absolute file

        List<Path> paths = getTopicPaths(fs, absolutePath);

        logger.info("Processing topics:{}", paths.stream()
            .map(p -> "\n  - " + p.getName())
            .collect(Collectors.joining()));

        processedFileCount = new LongAdder();
        processedRecordsCount = new LongAdder();

        ExecutorService executor = Executors.newWorkStealingPool(this.numThreads);

        paths.forEach(p -> executor.execute(() -> {
            String topic = p.getName();
            logger.info("Processing topic {}", p);
            try (RemoteLock ignored = lockManager.acquireTopicLock(topic);
                    Accountant accountant = new Accountant(fileStoreFactory, topic)) {
                // Get filenames to process
                OffsetRangeSet seenFiles = accountant.getOffsets();
                TopicFileList topicPaths =  new TopicFileList(walk(fs, p)
                        .filter(f -> f.getName().endsWith(".avro"))
                        .map(f -> new TopicFile(topic, f))
                        .filter(f -> !seenFiles.contains(f.range))
                        .limit(maxFilesPerTopic));

                processPaths(topicPaths, accountant);
            } catch (IOException ex) {
                logger.error("Failed to map files of topic {}", topic, ex);
            }
        }));

        executor.shutdown();
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
    }

    private List<Path> getTopicPaths(FileSystem fs, Path path) {
        List<Path> topics = findTopicPaths(fs, path)
                .distinct()
                .filter(f -> !excludeTopics.contains(f.getName()))
                .collect(Collectors.toList());

        Collections.shuffle(topics);
        return topics;
    }

    private Stream<Path> walk(FileSystem fs, Path path) {
        FileStatus[] files;
        try {
            files = fs.listStatus(path);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
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

    private Stream<Path> findTopicPaths(FileSystem fs, Path path) {
        FileStatus[] files;
        try {
            files = fs.listStatus(path);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return Stream.of(files)
                .parallel()
                .flatMap(f -> {
                    Path p = f.getPath();
                    String filename = p.getName();
                    if (f.isDirectory()) {
                        if (filename.equals("+tmp")) {
                            return Stream.empty();
                        } else {
                            return findTopicPaths(fs, p);
                        }
                    } else if (filename.endsWith(".avro")) {
                        return Stream.of(p.getParent().getParent());
                    } else {
                        return Stream.empty();
                    }
                })
                .distinct();
    }

    private void processPaths(TopicFileList topicPaths, Accountant accountant) {
        int numFiles = topicPaths.numberOfFiles();
        long numOffsets = topicPaths.numberOfOffsets();

        logger.info("Converting {} files with {} records",
                numFiles, NumberFormat.getNumberInstance().format(numOffsets));

        OffsetRangeSet seenOffsets = accountant.getOffsets()
                .withFactory(ReadOnlyFunctionalValue::new);

        ProgressBar progressBar = new ProgressBar(topicPaths.files.get(0).topic, numOffsets, 50, 500, TimeUnit.MILLISECONDS);
        progressBar.update(0);

        // Actually process the files
        String size = NumberFormat.getNumberInstance().format(topicPaths.size);
        String topic = topicPaths.files.get(0).topic;
        logger.info("Processing {} records for topic {}", size, topic);
        long batchSize = Math.round(BATCH_SIZE * ThreadLocalRandom.current().nextDouble(0.75, 1.25));
        long currentSize = 0;
        try (FileCacheStore cache = fileStoreFactory.newFileCacheStore(accountant)) {
            for (TopicFile file : topicPaths.files) {
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
        } catch (IOException | UncheckedIOException ex) {
            logger.error("Failed to process file", ex);
        } catch (IllegalStateException ex) {
            logger.warn("Shutting down");
        }

        progressBar.update(numOffsets);
    }

    private void processFile(TopicFile file, FileCacheStore cache,
            ProgressBar progressBar, OffsetRangeSet seenOffsets) throws IOException {
        logger.debug("Reading {}", file.path);

        // Read and parseFilename avro file
        FsInput input = new FsInput(file.path, conf);

        // processing zero-length files may trigger a stall. See:
        // https://github.com/RADAR-base/Restructure-HDFS-topic/issues/3
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
        Accountant.Transaction transaction = new Accountant.Transaction(topicPartition, offset);
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

        public TopicFileList(Stream<TopicFile> files) {
            this.files = files.collect(Collectors.toList());
            this.size = this.files.stream()
                    .mapToLong(TopicFile::size)
                    .sum();
        }

        public int numberOfFiles() {
            return this.files.size();
        }

        public long numberOfOffsets() {
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

        public long size() {
            return 1 + range.getOffsetTo() - range.getOffsetFrom();
        }
    }
}
