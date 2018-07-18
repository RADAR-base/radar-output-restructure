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

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
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
import org.radarcns.hdfs.data.Compression;
import org.radarcns.hdfs.data.CompressionFactory;
import org.radarcns.hdfs.data.FileCacheStore;
import org.radarcns.hdfs.data.FormatFactory;
import org.radarcns.hdfs.data.LocalStorageDriver;
import org.radarcns.hdfs.data.RecordConverterFactory;
import org.radarcns.hdfs.data.StorageDriver;
import org.radarcns.hdfs.util.ProgressBar;
import org.radarcns.hdfs.util.commandline.CommandLineArgs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.function.Supplier;

public class RestructureAvroRecords {
    private static final Logger logger = LoggerFactory.getLogger(RestructureAvroRecords.class);

    private static final java.nio.file.Path OFFSETS_FILE_NAME = Paths.get("offsets.csv");
    private static final java.nio.file.Path BINS_FILE_NAME = Paths.get("bins.csv");
    private static final java.nio.file.Path SCHEMA_OUTPUT_FILE_NAME = Paths.get("schema.json");
    private static final SimpleDateFormat FILE_DATE_FORMAT = new SimpleDateFormat("yyyyMMdd_HH");

    static {
        FILE_DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    private final RecordConverterFactory converterFactory;
    private final RecordPathFactory pathFactory;
    private final Compression compression;
    private final StorageDriver storageDriver;

    private java.nio.file.Path offsetsPath;
    private Frequency bins;

    private final Configuration conf;

    private long processedFileCount;
    private long processedRecordsCount;
    private final boolean doDeduplicate;

    public static void main(String [] args) {

        final CommandLineArgs commandLineArgs = new CommandLineArgs();
        final JCommander parser = JCommander.newBuilder().addObject(commandLineArgs).build();

        parser.setProgramName("radar-hdfs-restructure");
        try {
            parser.parse(args);
        } catch (ParameterException ex) {
            logger.error(ex.getMessage());
            parser.usage();
            System.exit(1);
        }

        if (commandLineArgs.help) {
            parser.usage();
            System.exit(0);
        }

        logger.info(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
        logger.info("Starting...");

        long time1 = System.currentTimeMillis();

        RestructureAvroRecords restr;

        try {
            restr = new Builder(commandLineArgs.hdfsUri, commandLineArgs.outputDirectory)
                    .compression(commandLineArgs.compression)
                    .doDeduplicate(commandLineArgs.deduplicate)
                    .format(commandLineArgs.format)
                    .hdfsHighAvailability(commandLineArgs.hdfsHa,
                            commandLineArgs.hdfsUri1, commandLineArgs.hdfsUri2)
                    .pathFactory(commandLineArgs.pathFactory)
                    .compressionFactory(commandLineArgs.compressionFactory)
                    .formatFactory(commandLineArgs.formatFactory)
                    .storageDriver(commandLineArgs.storageDriver)
                    .properties(commandLineArgs.properties)
                    .build();
        } catch (IllegalArgumentException ex) {
            logger.error("HDFS High availability name node configuration is incomplete."
                    + " Configure --namenode-1, --namenode-2 and --namenode-ha");
            System.exit(1);
            return;
        } catch (IOException ex) {
            logger.error("Failed to initialize plugins", ex);
            System.exit(1);
            return;
        } catch (ReflectiveOperationException | ClassCastException e) {
            logger.error("Cannot find factory", e);
            System.exit(1);
            return;
        }

        try {
            for(String input : commandLineArgs.inputPaths) {
                logger.info("In:  hdfs://" + commandLineArgs.hdfsUri + input);
                logger.info("Out: " + commandLineArgs.outputDirectory);
                restr.start(input);
            }
        } catch (IOException ex) {
            logger.error("Processing failed", ex);
        }
        logger.info("Processed {} files and {} records", restr.getProcessedFileCount(), restr.getProcessedRecordsCount());
        logger.info("Time taken: {} seconds", (System.currentTimeMillis() - time1)/1000d);
    }

    private RestructureAvroRecords(RestructureAvroRecords.Builder builder) {
        this.pathFactory = builder.pathFactory;

        this.setInputWebHdfsURL(builder.hdfsUri);
        this.setOutputPath(builder.outputPath);

        conf = new Configuration();
        for (Map.Entry<String, String> hdfsConf : builder.hdfsConf.entrySet()) {
            conf.set(hdfsConf.getKey(), hdfsConf.getValue());
        }

        this.doDeduplicate = builder.doDeduplicate;
        logger.info("Deduplicate set to {}", doDeduplicate);

        converterFactory = builder.formatFactory.get(builder.format);
        compression = builder.compressionFactory.get(builder.compression);
        storageDriver = builder.storageDriver;

        String extension = converterFactory.getExtension() + compression.getExtension();
        this.pathFactory.setExtension(extension);
    }

    public void setInputWebHdfsURL(String fileSystemURL) {
        conf.set("fs.defaultFS", "hdfs://" + fileSystemURL);
    }

    public void setOutputPath(String path) {
        // Remove trailing backslash
        java.nio.file.Path outputPath = Paths.get(path.replaceAll("/$", ""));
        offsetsPath = outputPath.resolve(OFFSETS_FILE_NAME);
        pathFactory.setRoot(outputPath);
        bins = Frequency.read(outputPath.resolve(BINS_FILE_NAME));
    }

    public long getProcessedFileCount() {
        return processedFileCount;
    }

    public long getProcessedRecordsCount() {
        return processedRecordsCount;
    }

    public void start(String directoryName) throws IOException {
        // Get files and directories
        Path path = new Path(directoryName);
        FileSystem fs = FileSystem.get(conf);

        try (OffsetRangeFile.Writer offsets = new OffsetRangeFile.Writer(offsetsPath)) {
            OffsetRangeSet seenFiles;
            try {
                seenFiles = OffsetRangeFile.read(offsetsPath);
            } catch (IOException ex) {
                logger.error("Error reading offsets file. Processing all offsets.");
                seenFiles = new OffsetRangeSet();
            }
            logger.info("Retrieving file list from {}", path);
            // Get filenames to process
            Map<String, List<Path>> topicPaths = new HashMap<>();
            long toProcessFileCount = 0L;
            processedFileCount = 0L;
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

            // Actually process the files
            for (Map.Entry<String, List<Path>> entry : topicPaths.entrySet()) {
                try (FileCacheStore cache = new FileCacheStore(storageDriver, converterFactory, 100, compression, doDeduplicate)) {
                    for (Path filePath : entry.getValue()) {
                        // If JsonMappingException occurs, log the error and continue with other files
                        try {
                            this.processFile(filePath, entry.getKey(), cache, offsets);
                        } catch (JsonMappingException exc) {
                            logger.error("Cannot map values", exc);
                        }
                        progressBar.update(++processedFileCount);
                    }
                }
            }
        }

        logger.info("Cleaning offset file");
        OffsetRangeFile.cleanUp(offsetsPath);
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
            this.writeRecord(record, topicName, cache, 0);
        }

        // Write which file has been processed and update bins
        try {
            OffsetRange range = OffsetRange.parseFilename(filePath.getName());
            offsets.write(range);
            bins.write();
        } catch (IOException ex) {
            logger.warn("Failed to update status. Continuing processing.", ex);
        }
    }

    private void writeRecord(GenericRecord record, String topicName, FileCacheStore cache, int suffix)
            throws IOException {
        GenericRecord keyField = (GenericRecord) record.get("key");
        GenericRecord valueField = (GenericRecord) record.get("value");

        if (keyField == null || valueField == null) {
            logger.error("Failed to process {}", record);
            throw new IOException("Failed to process " + record + "; no key or value");
        }

        RecordPathFactory.RecordOrganization metadata = pathFactory.getRecordOrganization(topicName, record, suffix);

        // Write data
        FileCacheStore.WriteResponse response = cache.writeRecord(metadata.getPath(), record);

        if (!response.isSuccessful()) {
            // Write was unsuccessful due to different number of columns,
            // try again with new file name
            writeRecord(record, topicName, cache, ++suffix);
        } else {
            // Write was successful, finalize the write
            java.nio.file.Path schemaPath = metadata.getPath().resolveSibling(SCHEMA_OUTPUT_FILE_NAME);
            if (!Files.exists(schemaPath)) {
                try (Writer writer = Files.newBufferedWriter(schemaPath)) {
                    writer.write(record.getSchema().toString(true));
                }
            }

            // Count data (binned and total)
            bins.add(topicName, metadata.getCategory(), pathFactory.getTimeBin(metadata.getTime()));
            processedRecordsCount++;
        }
    }

    @SuppressWarnings({"UnusedReturnValue", "WeakerAccess"})
    public static class Builder {
        private boolean doDeduplicate;
        private String hdfsUri;
        private final Map<String, String> hdfsConf = new HashMap<>();
        private String outputPath;
        private String format;
        private String compression;
        private RecordPathFactory pathFactory;
        private StorageDriver storageDriver;
        private CompressionFactory compressionFactory;
        private FormatFactory formatFactory;
        private final Map<String, String> properties = new HashMap<>();

        public Builder(final String uri, final String outputPath) {
            this.hdfsUri = uri;
            this.outputPath = outputPath;
        }

        public Builder compression(final String compression) {
            this.compression = compression;
            return this;
        }

        public Builder doDeduplicate(final boolean dedup) {
            this.doDeduplicate = dedup;
            return this;
        }

        public Builder format(final String format) {
            this.format = format;
            return this;
        }

        public Builder storageDriver(String storageDriver)
                throws ReflectiveOperationException, ClassCastException {
            this.storageDriver = instantiate(storageDriver, StorageDriver.class);
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

        public Builder pathFactory(String factoryClassName)
                throws ReflectiveOperationException, ClassCastException {
            this.pathFactory = instantiate(factoryClassName, RecordPathFactory.class);
            return this;
        }

        public Builder compressionFactory(String factoryClassName)
                throws ReflectiveOperationException, ClassCastException {
            this.compressionFactory = instantiate(factoryClassName, CompressionFactory.class);
            return this;
        }

        public Builder formatFactory(String factoryClassName)
                throws ReflectiveOperationException, ClassCastException {
            this.formatFactory = instantiate(factoryClassName, FormatFactory.class);
            return this;
        }

        public Builder putHdfsConfig(String name, String value) {
            hdfsConf.put(name, value);
            return this;
        }

        public Builder properties(Map<String, String> props) {
            this.properties.putAll(props);
            return this;
        }

        private static <T> T instantiate(String clsName, Class<T> superClass)
                throws ReflectiveOperationException, ClassCastException {
            if (clsName == null) {
                return null;
            }
            Class cls = Class.forName(clsName);
            return superClass.cast(cls.newInstance());
        }

        public RestructureAvroRecords build() throws IOException {
            compression = getOrDefault(compression, () -> "identity");
            format = getOrDefault(format, () -> "csv");
            pathFactory = getOrDefault(pathFactory, ObservationKeyPathFactory::new);
            pathFactory.init(properties);
            storageDriver = getOrDefault(storageDriver, LocalStorageDriver::new);
            storageDriver.init(properties);
            compressionFactory = getOrDefault(compressionFactory, CompressionFactory::new);
            compressionFactory.init(properties);
            formatFactory = getOrDefault(formatFactory, FormatFactory::new);
            formatFactory.init(properties);

            return new RestructureAvroRecords(this);
        }

        private static <T> T getOrDefault(T value, Supplier<T> defaultValue) {
            return value != null ? value : defaultValue.get();
        }
    }
}
