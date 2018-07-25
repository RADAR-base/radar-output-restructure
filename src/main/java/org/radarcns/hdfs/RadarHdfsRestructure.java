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
import org.radarcns.hdfs.data.FileCacheStore;
import org.radarcns.hdfs.util.ProgressBar;
import org.radarcns.hdfs.util.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.radarcns.hdfs.util.Partitioners.unorderedBatches;
import static org.radarcns.hdfs.util.ProgressBar.formatTime;

public class RadarHdfsRestructure {
    private static final Logger logger = LoggerFactory.getLogger(RadarHdfsRestructure.class);

    private static final SimpleDateFormat FILE_DATE_FORMAT = new SimpleDateFormat("yyyyMMdd_HH");
    /** Number of offsets to process in a single task. */
    private static final int PARTITION_SIZE = 500_000;

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
            List<TopicFile> topicPaths = getTopicPaths(fs, path, accountant.getOffsets());
            logger.info("Time retrieving file list: {}",
                    formatTime(Duration.between(timeStart, Instant.now())));

            processPaths(topicPaths, accountant);
        } catch (InterruptedException e) {
            logger.error("Processing interrupted");
            Thread.currentThread().interrupt();
        }
    }

    private List<TopicFile> getTopicPaths(FileSystem fs, Path path, OffsetRangeSet seenFiles) {
        return walk(fs, path)
                .filter(f -> {
                    String name = f.getName();
                    return name.endsWith(".avro")
                            && !seenFiles.contains(OffsetRange.parseFilename(name));
                })
                .map(f -> new TopicFile(f.getParent().getParent().getName(), f))
                .collect(Collectors.toList());
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

    private void processPaths(List<TopicFile> topicPaths, Accountant accountant) throws InterruptedException {
        long toProcessRecordsCount = topicPaths.stream()
                .mapToInt(TopicFile::size)
                .sum();

        long toProcessFileCount = topicPaths.size();

        logger.info("Converting {} files with {} records",
                toProcessFileCount, toProcessRecordsCount);

        processedFileCount = new LongAdder();
        processedRecordsCount = new LongAdder();

        ExecutorService executor = Executors.newFixedThreadPool(pathFactory.isTopicPartitioned() ? this.numThreads : 1);

        ProgressBar progressBar = new ProgressBar(toProcessRecordsCount, 70, 100, TimeUnit.MILLISECONDS);

        // Actually process the files
        partitionPaths(topicPaths).entrySet().stream()
                // sort in ascending order of number of partitions
                // to ensure that largest number of partitions go first
                .sorted(Comparator.comparing(e -> -e.getValue().size()))
                .forEach(e -> {
                    logger.info("Processing {} partitions for topic {}", e.getValue().size(), e.getKey());
                    executor.execute(() -> e.getValue().forEach(paths -> {
                            try (FileCacheStore cache = fileStoreFactory.newFileCacheStore(accountant)) {
                                for (TopicFile file : paths) {
                                    // If JsonMappingException occurs, log the error and continue with other files
                                    try {
                                        this.processFile(file, cache, progressBar);
                                    } catch (JsonMappingException exc) {
                                        logger.error("Cannot map values", exc);
                                    }
                                    processedFileCount.increment();
                                }
                            } catch (IOException ex) {
                                logger.error("Failed to process file", ex);
                            }
                        }));
                });

        progressBar.update(0);

        executor.shutdown();
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
        progressBar.update(toProcessRecordsCount);
    }

    /** Partition the paths into sets of 10,000 records. */
    private Map<String, List<List<TopicFile>>> partitionPaths(List<TopicFile> allPaths) {
        return allPaths.stream()
                .collect(Collectors.groupingBy(TopicFile::getTopic,
                        unorderedBatches(RadarHdfsRestructure.PARTITION_SIZE, TopicFile::size, Collectors.toList())));
    }

    private void processFile(TopicFile file, FileCacheStore cache,
            ProgressBar progressBar) throws IOException {
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
        int i = 0;
        while (dataFileReader.hasNext()) {
            record = dataFileReader.next(record);
            timer.add("read", timeRead);

            long timeAccount = System.nanoTime();
            OffsetRange singleOffset = file.range.createSingleOffset(i++);
            timer.add("accounting.create", timeAccount);
            // Get the fields
            this.writeRecord(record, file.topic, cache, singleOffset, 0);

            processedRecordsCount.increment();
            progressBar.update(processedRecordsCount.sum());
            timeRead = System.nanoTime();
        }
    }

    private void writeRecord(GenericRecord record, String topicName, FileCacheStore cache,
            OffsetRange offset, int suffix) throws IOException {
        RecordPathFactory.RecordOrganization metadata = pathFactory.getRecordOrganization(topicName, record, suffix);

        Timer timer = Timer.getInstance();

        long timeAccount = System.nanoTime();
        String timeBin = pathFactory.getTimeBin(metadata.getTime());
        Accountant.Transaction transaction = new Accountant.Transaction(offset,
                new Bin(topicName, metadata.getCategory(), timeBin));
        timer.add("accounting.create", timeAccount);

        // Write data
        long timeWrite = System.nanoTime();
        FileCacheStore.WriteResponse response = cache.writeRecord(
                topicName, metadata.getPath(), record, transaction);
        timer.add("write", timeWrite);

        if (!response.isSuccessful()) {
            // Write was unsuccessful due to different number of columns,
            // try again with new file name
            writeRecord(record, topicName, cache, offset, ++suffix);
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
