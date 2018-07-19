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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.radarcns.hdfs.data.FileCacheStore;
import org.radarcns.hdfs.data.StorageDriver;
import org.radarcns.hdfs.util.ProgressBar;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.TimeZone;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

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
    private Frequency bins;

    private final Configuration conf;
    private final java.nio.file.Path outputPath;

    private LongAdder processedFileCount;
    private LongAdder processedRecordsCount;
    private FileStoreFactory fileStoreFactory;
    private RecordPathFactory pathFactory;
    private StorageDriver storage;

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

        try (OffsetRangeFile.Writer offsets = new OffsetRangeFile.Writer(storage, offsetsPath)) {
            OffsetRangeSet seenFiles;
            try {
                seenFiles = OffsetRangeFile.read(storage, offsetsPath);
            } catch (IOException ex) {
                logger.error("Error reading offsets file. Processing all offsets.");
                seenFiles = new OffsetRangeSet();
            }

            // Get filenames to process
            Map<String, List<Path>> topicPaths = getTopicPaths(fs, path, seenFiles);

            long toProcessFileCount = topicPaths.values().stream()
                    .mapToInt(List::size)
                    .sum();

            processedFileCount = new LongAdder();
            processedRecordsCount = new LongAdder();

            RemoteIterator<LocatedFileStatus> files = fs.listFiles(path, true);
            while (files.hasNext()) {
                LocatedFileStatus locatedFileStatus = files.next();
                if (locatedFileStatus.isDirectory()) {
                    continue;
                }
                Path filePath = locatedFileStatus.getPath();

                String topic = getTopic(filePath, seenFiles);
                if (topic != null) {
                    topicPaths.computeIfAbsent(topic, k -> new ArrayList<>()).add(filePath);
                    toProcessFileCount++;
                }
            }

            logger.info("Converting {} files", toProcessFileCount);

            ProgressBar progressBar = new ProgressBar(toProcessFileCount, 70);
            progressBar.update(0);

            logger.info("Retrieving file list from {}", path);
            ExecutorService executor = Executors.newFixedThreadPool(getPathFactory().isTopicPartitioned() ? this.numThreads : 1);

            // Actually process the files
            topicPaths.forEach((topic, paths) -> executor.submit(() -> {
                try (FileCacheStore cache = fileStoreFactory.newFileCacheStore()) {
                    for (Path filePath : paths) {
                        // If JsonMappingException occurs, log the error and continue with other files
                        try {
                            this.processFile(filePath, topic, cache, offsets);
                        } catch (JsonMappingException exc) {
                            logger.error("Cannot map values", exc);
                        }
                        processedFileCount.increment();
                        progressBar.update(processedFileCount.sum());
                    }
                } catch (IOException ex) {
                    logger.error("Failed to process file", ex);
                }
            }));

            executor.shutdown();
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.error("Processing interrupted");
            Thread.currentThread().interrupt();
        }

        logger.info("Cleaning offset file");
        OffsetRangeFile.cleanUp(storage, offsetsPath);
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

        System.setProperty("java.util.concurrent.ForkJoinPool.common.parallelism", oldParallelism);

        return result;
    }

    private Stream<Path> walk(FileSystem fs, Path path) {
        RemoteIterator<LocatedFileStatus> files;
        try {
            files = fs.listFiles(path, false);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        Spliterator<LocatedFileStatus> spliterator = Spliterators.spliteratorUnknownSize(
                new Iterator<LocatedFileStatus>() {
                    @Override
                    public boolean hasNext() {
                        try {
                            return files.hasNext();
                        } catch (IOException e) {
                            throw new IllegalStateException(e);
                        }
                    }

                    @Override
                    public LocatedFileStatus next() {
                        try {
                            return files.next();
                        } catch (IOException e) {
                            throw new IllegalStateException(e);
                        }
                    }
                }, Spliterator.NONNULL | Spliterator.IMMUTABLE | Spliterator.DISTINCT);

        return StreamSupport.stream(spliterator, true)
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

    private static String getTopic(Path filePath, OffsetRangeSet seenFiles) {
        if (filePath.toString().contains("+tmp")) {
            return null;
        }

        String fileName = filePath.getName();
        // Skip if extension is not .avro
        if (!fileName.endsWith(".avro")) {
            logger.info("Skipping non-avro file: {}", fileName);
            return null;
        }

        OffsetRange range = OffsetRange.parseFilename(fileName);
        // Skip already processed avro files
        if (seenFiles.contains(range)) {
            return null;
        }

        return filePath.getParent().getParent().getName();
    }

    private void processFile(Path filePath, String topicName, FileCacheStore cache,
            OffsetRangeFile.Writer offsets) throws IOException {
        logger.debug("Reading {}", filePath);

        // Read and parseFilename avro file
        FsInput input = new FsInput(filePath, conf);

        // processing zero-length files may trigger a stall. See:
        // https://github.com/RADAR-CNS/Restructure-HDFS-topic/issues/3
        if (input.length() == 0) {
            logger.warn("File {} has zero length, skipping.", filePath);
            return;
        }

        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(input,
                new GenericDatumReader<>());

        GenericRecord record = null;
        while (dataFileReader.hasNext()) {
            record = dataFileReader.next(record);

            // Get the fields
            this.writeRecord(record, topicName, cache, offsets, filePath.getName(), 0);
        }
    }

    private void writeRecord(GenericRecord record, String topicName, FileCacheStore cache,
            OffsetRangeFile.Writer offsets, String filename, int suffix) throws IOException {
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
            bins.add(topicName, metadata.getCategory(), timeBin);
            OffsetRange range = OffsetRange.parseFilename(filename);
            try {
                offsets.write(range);
            } catch (IOException ex) {
                logger.warn("Failed to update offset status. Continuing processing.", ex);
            }
        }, () -> bins.write());

        if (!response.isSuccessful()) {
            // Write was unsuccessful due to different number of columns,
            // try again with new file name
            writeRecord(record, topicName, cache, offsets, filename, ++suffix);
        } else {
            // Write was successful, finalize the write
            java.nio.file.Path schemaPath = metadata.getPath().resolveSibling(SCHEMA_OUTPUT_FILE_NAME);
            if (!storage.exists(schemaPath)) {
                try (OutputStream out = storage.newOutputStream(schemaPath, false);
                     Writer writer = new OutputStreamWriter(out)) {
                    writer.write(record.getSchema().toString(true));
                }
            }

            processedRecordsCount.increment();
        }
    }

    public synchronized void setFileStoreFactory(FileStoreFactory factory) {
        this.fileStoreFactory = factory;
        this.pathFactory = factory.getPathFactory();
        this.storage = factory.getStorageDriver();
        bins = Frequency.read(storage, outputPath.resolve(BINS_FILE_NAME));
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
