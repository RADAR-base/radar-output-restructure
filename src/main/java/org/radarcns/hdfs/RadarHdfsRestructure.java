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
import org.radarcns.hdfs.data.StorageDriver;
import org.radarcns.hdfs.util.ProgressBar;
import org.radarcns.hdfs.util.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.radarcns.hdfs.util.ProgressBar.formatTime;

public class RadarHdfsRestructure {
    private static final Logger logger = LoggerFactory.getLogger(RadarHdfsRestructure.class);

    private static final java.nio.file.Path SCHEMA_OUTPUT_FILE_NAME = Paths.get("schema.json");
    private static final SimpleDateFormat FILE_DATE_FORMAT = new SimpleDateFormat("yyyyMMdd_HH");

    static {
        FILE_DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    private final int numThreads;
    private final Configuration conf;
    private final FileStoreFactory fileStoreFactory;
    private final RecordPathFactory pathFactory;
    private final StorageDriver storage;

    private LongAdder processedFileCount;
    private LongAdder processedRecordsCount;

    public RadarHdfsRestructure(FileStoreFactory factory) {
        conf = factory.getHdfsSettings().getConfiguration();
        conf.set("fs.defaultFS", "hdfs://" + factory.getHdfsSettings().getHdfsName());
        this.numThreads = factory.getSettings().getNumThreads();
        this.fileStoreFactory = factory;
        this.pathFactory = factory.getPathFactory();
        this.storage = factory.getStorageDriver();
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
            Map<String, List<Path>> topicPaths = getTopicPaths(fs, path, accountant.getOffsets());
            logger.info("Time retrieving file list: {}",
                    formatTime(Duration.between(timeStart, Instant.now())));

            processPaths(topicPaths, accountant);
        } catch (InterruptedException e) {
            logger.error("Processing interrupted");
            Thread.currentThread().interrupt();
        }
    }

    private Map<String, List<Path>> getTopicPaths(FileSystem fs, Path path, OffsetRangeSet seenFiles) {
        String oldParallelism = System.getProperty("java.util.concurrent.ForkJoinPool.common.parallelism");
        System.setProperty("java.util.concurrent.ForkJoinPool.common.parallelism", String.valueOf(numThreads - 1));

        Map<String, List<Path>> result = walk(fs, path)
                .filter(f -> {
                    String name = f.getName();
                    return name.endsWith(".avro")
                            && !seenFiles.contains(OffsetRange.parseFilename(name));
                })
                .collect(Collectors.groupingByConcurrent(f -> f.getParent().getParent().getName()));

        if (oldParallelism == null) {
            System.clearProperty("java.util.concurrent.ForkJoinPool.common.parallelism");
        } else {
            System.setProperty("java.util.concurrent.ForkJoinPool.common.parallelism", oldParallelism);
        }

        return result;
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

    private void processPaths(Map<String, List<Path>> topicPaths, Accountant accountant) throws InterruptedException {
        long toProcessRecordsCount = topicPaths.values().parallelStream()
                .flatMap(List::stream)
                .map(p -> OffsetRange.parseFilename(p.getName()))
                .mapToInt(r -> (int)(r.getOffsetTo() - r.getOffsetFrom()) + 1)
                .sum();

        long toProcessFileCount = topicPaths.values().parallelStream()
                .mapToInt(List::size)
                .sum();

        logger.info("Converting {} files with {} records",
                toProcessFileCount, toProcessRecordsCount);

        processedFileCount = new LongAdder();
        processedRecordsCount = new LongAdder();

        ProgressBar progressBar = new ProgressBar(toProcessRecordsCount, 70);
        progressBar.update(0);
        AtomicLong lastUpdate = new AtomicLong(0L);

        ExecutorService executor = Executors.newWorkStealingPool(pathFactory.isTopicPartitioned() ? this.numThreads : 1);

        // Actually process the files
        topicPaths.forEach((topic, paths) -> executor.submit(() -> {
            try (FileCacheStore cache = fileStoreFactory.newFileCacheStore(accountant)) {
                for (Path filePath : paths) {
                    // If JsonMappingException occurs, log the error and continue with other files
                    try {
                        this.processFile(filePath, topic, cache, progressBar, lastUpdate);
                    } catch (JsonMappingException exc) {
                        logger.error("Cannot map values", exc);
                    }
                    processedFileCount.increment();
                }
                accountant.flush();
            } catch (IOException ex) {
                logger.error("Failed to process file", ex);
            }
        }));

        executor.shutdown();
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
        progressBar.update(toProcessRecordsCount);
    }

    private void processFile(Path filePath, String topicName, FileCacheStore cache,
            ProgressBar progressBar, AtomicLong lastUpdate) throws IOException {
        logger.debug("Reading {}", filePath);

        // Read and parseFilename avro file
        FsInput input = new FsInput(filePath, conf);

        // processing zero-length files may trigger a stall. See:
        // https://github.com/RADAR-CNS/Restructure-HDFS-topic/issues/3
        if (input.length() == 0) {
            logger.warn("File {} has zero length, skipping.", filePath);
            return;
        }

        OffsetRange offsets = OffsetRange.parseFilename(filePath.getName());

        long timeRead = System.nanoTime();
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(input,
                new GenericDatumReader<>());

        GenericRecord record = null;
        int i = 0;
        while (dataFileReader.hasNext()) {
            record = dataFileReader.next(record);
            Timer.getInstance().add("read", timeRead);

            OffsetRange singleOffset = offsets.createSingleOffset(i++);
            // Get the fields
            this.writeRecord(record, topicName, cache, singleOffset, 0);

            processedRecordsCount.increment();
            long now = System.nanoTime();
            if (now == lastUpdate.updateAndGet(l -> now > l + 100_000_000L ? now : l)) {
                progressBar.update(processedRecordsCount.sum());
            }
            timeRead = System.nanoTime();
        }
    }

    private void writeRecord(GenericRecord record, String topicName, FileCacheStore cache,
            OffsetRange offset, int suffix) throws IOException {
        RecordPathFactory.RecordOrganization metadata = pathFactory.getRecordOrganization(topicName, record, suffix);

        String timeBin = pathFactory.getTimeBin(metadata.getTime());
        Bin bin = new Bin(topicName, metadata.getCategory(), timeBin);

        // Write data
        long timeWrite = System.nanoTime();
        FileCacheStore.WriteResponse response = cache.writeRecord(
                metadata.getPath(), record, new Accountant.Transaction(offset, bin));
        Timer.getInstance().add("write", timeWrite);

        if (!response.isSuccessful()) {
            // Write was unsuccessful due to different number of columns,
            // try again with new file name
            writeRecord(record, topicName, cache, offset, ++suffix);
        } else {
            // Write was successful, finalize the write
            java.nio.file.Path schemaPath = metadata.getPath().resolveSibling(SCHEMA_OUTPUT_FILE_NAME);
            if (!storage.exists(schemaPath)) {
                try (OutputStream out = storage.newOutputStream(schemaPath, false);
                     Writer writer = new OutputStreamWriter(out)) {
                    writer.write(record.getSchema().toString(true));
                }
            }
        }
    }
}
