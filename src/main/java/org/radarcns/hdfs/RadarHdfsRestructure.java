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
import java.util.HashMap;
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

    private static final java.nio.file.Path OFFSETS_FILE_NAME = Paths.get("offsets.csv");
    private static final java.nio.file.Path BINS_FILE_NAME = Paths.get("bins.csv");
    private static final java.nio.file.Path SCHEMA_OUTPUT_FILE_NAME = Paths.get("schema.json");
    private static final SimpleDateFormat FILE_DATE_FORMAT = new SimpleDateFormat("yyyyMMdd_HH");

    static {
        FILE_DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    private final int numThreads;

    private final java.nio.file.Path offsetsPath;

    private final Configuration conf;
    private final java.nio.file.Path outputPath;

    private LongAdder processedFileCount;
    private LongAdder processedRecordsCount;
    private FileStoreFactory fileStoreFactory;
    private RecordPathFactory pathFactory;
    private StorageDriver storage;
    private LongAdder filesClosed;

    private RadarHdfsRestructure(Builder builder) {
        conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://" + builder.hdfsUri);
        outputPath = Paths.get(builder.root.replaceAll("/$", ""));
        offsetsPath = outputPath.resolve(OFFSETS_FILE_NAME);
        this.numThreads = builder.numThreads;

        for (Map.Entry<String, String> hdfsConf : builder.hdfsConf.entrySet()) {
            conf.set(hdfsConf.getKey(), hdfsConf.getValue());
        }
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

        try (OffsetRangeFile offsetFile = OffsetRangeFile.read(storage, offsetsPath);
             Frequency bins = Frequency.read(storage, outputPath.resolve(BINS_FILE_NAME))) {

            logger.info("Retrieving file list from {}", path);

            Instant timeStart = Instant.now();
            // Get filenames to process
            Map<String, List<Path>> topicPaths = getTopicPaths(fs, path, offsetFile.getOffsets());
            logger.info("Time retrieving file list: {}",
                    formatTime(Duration.between(timeStart, Instant.now())));

            processPaths(topicPaths, offsetFile, bins);
        } catch (InterruptedException e) {
            logger.error("Processing interrupted");
            Thread.currentThread().interrupt();
        }

        logger.debug("Files closed: {}", filesClosed.sum());
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

    private void processPaths(Map<String, List<Path>> topicPaths, OffsetRangeFile offsetFile, Frequency bins) throws InterruptedException {
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
        filesClosed = new LongAdder();

        ProgressBar progressBar = new ProgressBar(toProcessRecordsCount, 70);
        progressBar.update(0);
        AtomicLong lastUpdate = new AtomicLong(0L);

        ExecutorService executor = Executors.newWorkStealingPool(getPathFactory().isTopicPartitioned() ? this.numThreads : 1);

        // Actually process the files
        topicPaths.forEach((topic, paths) -> executor.submit(() -> {
            try (FileCacheStore cache = fileStoreFactory.newFileCacheStore()) {
                for (Path filePath : paths) {
                    // If JsonMappingException occurs, log the error and continue with other files
                    try {
                        this.processFile(filePath, topic, cache, offsetFile, bins, progressBar, lastUpdate);
                    } catch (JsonMappingException exc) {
                        logger.error("Cannot map values", exc);
                    }
                    processedFileCount.increment();
                }
                bins.flush();
                offsetFile.flush();
            } catch (IOException ex) {
                logger.error("Failed to process file", ex);
            }
        }));

        executor.shutdown();
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
        progressBar.update(toProcessRecordsCount);
    }

    private void processFile(Path filePath, String topicName, FileCacheStore cache,
            OffsetRangeFile offsets, Frequency bins, ProgressBar progressBar, AtomicLong lastUpdate) throws IOException {
        logger.debug("Reading {}", filePath);

        // Read and parseFilename avro file
        FsInput input = new FsInput(filePath, conf);

        // processing zero-length files may trigger a stall. See:
        // https://github.com/RADAR-CNS/Restructure-HDFS-topic/issues/3
        if (input.length() == 0) {
            logger.warn("File {} has zero length, skipping.", filePath);
            return;
        }

        long timeRead = System.nanoTime();
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(input,
                new GenericDatumReader<>());

        GenericRecord record = null;
        while (dataFileReader.hasNext()) {
            record = dataFileReader.next(record);
            Timer.getInstance().add("read", System.nanoTime() - timeRead);

            // Get the fields
            this.writeRecord(record, topicName, cache, offsets, bins, filePath.getName(), 0);

            processedRecordsCount.increment();
            long now = System.nanoTime();
            if (now == lastUpdate.updateAndGet(l -> now > l + 100_000_000L ? now : l)) {
                progressBar.update(processedRecordsCount.sum());
            }
            timeRead = System.nanoTime();
        }
    }

    private void writeRecord(GenericRecord record, String topicName, FileCacheStore cache,
            OffsetRangeFile offsets, Frequency bins, String filename, int suffix) throws IOException {
        GenericRecord keyField = (GenericRecord) record.get("key");
        GenericRecord valueField = (GenericRecord) record.get("value");

        if (keyField == null || valueField == null) {
            logger.error("Failed to process {}", record);
            throw new IOException("Failed to process " + record + "; no key or value");
        }

        RecordPathFactory.RecordOrganization metadata = pathFactory.getRecordOrganization(topicName, record, suffix);

        // Write data
        FileCacheStore.WriteResponse response = cache.writeRecord(metadata.getPath(), record, () -> {
            // Count data (binned and total)
            String timeBin = getPathFactory().getTimeBin(metadata.getTime());
            bins.increment(new Frequency.Bin(topicName, metadata.getCategory(), timeBin));
            offsets.add(OffsetRange.parseFilename(filename));
        }, () -> {
            offsets.triggerWrite();
            bins.triggerWrite();
            filesClosed.increment();
        });

        if (!response.isSuccessful()) {
            // Write was unsuccessful due to different number of columns,
            // try again with new file name
            writeRecord(record, topicName, cache, offsets, bins, filename, ++suffix);
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

    public synchronized void setFileStoreFactory(FileStoreFactory factory) {
        this.fileStoreFactory = factory;
        this.pathFactory = factory.getPathFactory();
        this.storage = factory.getStorageDriver();
    }

    private synchronized RecordPathFactory getPathFactory() {
        return pathFactory;
    }

    @SuppressWarnings({"UnusedReturnValue", "WeakerAccess"})
    public static class Builder {
        public String root;
        private String hdfsUri;
        private final Map<String, String> hdfsConf = new HashMap<>();
        private int numThreads = 1;

        public Builder(final String uri, String root) {
            this.hdfsUri = uri;
            this.root = root;
        }

        public Builder numThreads(int num) {
            if (num < 1) {
                throw new IllegalArgumentException("Number of threads must be at least 1");
            }
            this.numThreads = num;
            return this;
        }

        public Builder hdfsHighAvailability(String hdfsHa, String hdfsNameNode1, String hdfsNameNode2) {
            // Configure high availability
            if (hdfsHa == null && hdfsNameNode1 == null && hdfsNameNode2 == null) {
                return this;
            }
            if (hdfsHa == null || hdfsNameNode1 == null || hdfsNameNode2 == null) {
                throw new IllegalArgumentException("HDFS High availability name node configuration is incomplete.");
            }

            String[] haNames = hdfsHa.split(",");
            if (haNames.length != 2) {
                logger.error("Cannot process HDFS High Availability mode for other than two name nodes.");
                System.exit(1);
            }

            putHdfsConfig("dfs.nameservices", hdfsUri)
                    .putHdfsConfig("dfs.ha.namenodes." + hdfsUri, hdfsHa)
                    .putHdfsConfig("dfs.namenode.rpc-address." + hdfsUri + "." + haNames[0],
                            hdfsNameNode1 + ":8020")
                    .putHdfsConfig("dfs.namenode.rpc-address." + hdfsUri + "." + haNames[1],
                            hdfsNameNode2 + ":8020")
                    .putHdfsConfig("dfs.client.failover.proxy.provider." + hdfsUri,
                            "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");

            return this;
        }

        public Builder putHdfsConfig(String name, String value) {
            hdfsConf.put(name, value);
            return this;
        }

        public RadarHdfsRestructure build() {
            return new RadarHdfsRestructure(this);
        }
    }
}
