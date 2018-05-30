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

package org.radarcns;

import com.beust.jcommander.JCommander;
import com.fasterxml.jackson.databind.JsonMappingException;
import org.apache.avro.Schema.Field;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.radarcns.util.CsvAvroConverter;
import org.radarcns.util.FileCacheStore;
import org.radarcns.util.JsonAvroConverter;
import org.radarcns.util.ProgressBar;
import org.radarcns.util.RecordConverterFactory;
import org.radarcns.util.commandline.CommandLineArgs;
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

public class RestructureAvroRecords {
    private static final Logger logger = LoggerFactory.getLogger(RestructureAvroRecords.class);

    private final String outputFileExtension;
    private static final java.nio.file.Path OFFSETS_FILE_NAME = Paths.get("offsets.csv");
    private static final java.nio.file.Path BINS_FILE_NAME = Paths.get("bins.csv");
    private static final java.nio.file.Path SCHEMA_OUTPUT_FILE_NAME = Paths.get("schema.json");
    private static final SimpleDateFormat FILE_DATE_FORMAT = new SimpleDateFormat("yyyyMMdd_HH");

    static {
        FILE_DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    private final RecordConverterFactory converterFactory;

    private java.nio.file.Path outputPath;
    private java.nio.file.Path offsetsPath;
    private Frequency bins;

    private final Configuration conf = new Configuration();

    private long processedFileCount;
    private long processedRecordsCount;
    private final boolean useGzip;
    private final boolean doDeduplicate;

    public static void main(String [] args) throws Exception {

        final CommandLineArgs commandLineArgs = new CommandLineArgs();
        final JCommander parser = JCommander.newBuilder().addObject(commandLineArgs).build();

        parser.setProgramName("hadoop jar restructurehdfs-all-0.3.3.jar");
        parser.parse(args);

        if(commandLineArgs.help) {
            parser.usage();
            System.exit(0);
        }

        logger.info(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
        logger.info("Starting...");

        long time1 = System.currentTimeMillis();

        RestructureAvroRecords restr = new RestructureAvroRecords.Builder(commandLineArgs.hdfsUri,
                commandLineArgs.outputDirectory)
                .useGzip("gzip".equalsIgnoreCase(commandLineArgs.compression))
                .doDeuplicate(commandLineArgs.deduplicate).format(commandLineArgs.format)
                .build();

        try {
            for(String input : commandLineArgs.inputPaths) {
                logger.info("In:  " + commandLineArgs.hdfsUri + input);
                logger.info("Out: " + commandLineArgs.outputDirectory);
                restr.start(input);
            }
        } catch (IOException ex) {
            logger.error("Processing failed", ex);
        }

        logger.info("Processed {} files and {} records", restr.getProcessedFileCount(), restr.getProcessedRecordsCount());
        logger.info("Time taken: {} seconds", (System.currentTimeMillis() - time1)/1000d);
    }

    private RestructureAvroRecords(String hdfsUri, String outputPath, boolean gzip, boolean dedup, String format) {
        this.setInputWebHdfsURL(hdfsUri);
        this.setOutputPath(outputPath);

        this.useGzip = gzip;
        this.doDeduplicate = dedup;
        logger.info("Deduplicate set to {}", doDeduplicate);

        String extension;
        if (format.equalsIgnoreCase("json")) {
            logger.info("Writing output files in JSON format");
            converterFactory = JsonAvroConverter.getFactory();
            extension = "json";
        } else {
            logger.info("Writing output files in CSV format");
            converterFactory = CsvAvroConverter.getFactory();
            extension = "csv";
        }
        if (this.useGzip) {
            logger.info("Compressing output files in GZIP format");
            extension += ".gz";
        }
        outputFileExtension = extension;
    }

    public void setInputWebHdfsURL(String fileSystemURL) {
        conf.set("fs.defaultFS", fileSystemURL);
    }

    public void setOutputPath(String path) {
        // Remove trailing backslash
        outputPath = Paths.get(path.replaceAll("/$", ""));
        offsetsPath = outputPath.resolve(OFFSETS_FILE_NAME);
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

            ProgressBar progressBar = new ProgressBar(toProcessFileCount, 10);
            progressBar.update(0);

            // Actually process the files
            for (Map.Entry<String, List<Path>> entry : topicPaths.entrySet()) {
                try (FileCacheStore cache = new FileCacheStore(converterFactory, 100, useGzip, doDeduplicate)) {
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

        Date time = getDate(keyField, valueField);
        java.nio.file.Path outputFileName = createFilename(time, suffix);

        String projectId;

        if(keyField.get("projectId") == null) {
            projectId = "unknown-project";
        } else {
            // Clean Project id for use in final pathname
            projectId = keyField.get("projectId").toString().replaceAll("[^a-zA-Z0-9_-]+", "");
        }

        // Clean user id and create final output pathname
        String userId = keyField.get("userId").toString().replaceAll("[^a-zA-Z0-9_-]+", "");

        java.nio.file.Path projectDir = this.outputPath.resolve(projectId);
        java.nio.file.Path userDir = projectDir.resolve(userId);
        java.nio.file.Path userTopicDir = userDir.resolve(topicName);
        java.nio.file.Path outputPath = userTopicDir.resolve(outputFileName);

        // Write data
        int response = cache.writeRecord(outputPath, record);

        if (response == FileCacheStore.CACHE_AND_NO_WRITE || response == FileCacheStore.NO_CACHE_AND_NO_WRITE) {
            // Write was unsuccessful due to different number of columns,
            // try again with new file name
            writeRecord(record, topicName, cache, ++suffix);
        } else {
            // Write was successful, finalize the write
            java.nio.file.Path schemaPath = userTopicDir.resolve(SCHEMA_OUTPUT_FILE_NAME);
            if (!Files.exists(schemaPath)) {
                try (Writer writer = Files.newBufferedWriter(schemaPath)) {
                    writer.write(record.getSchema().toString(true));
                }
            }

            // Count data (binned and total)
            bins.add(topicName, keyField.get("sourceId").toString(), time);
            processedRecordsCount++;
        }
    }

    private java.nio.file.Path createFilename(Date date, int suffix) {
        if (date == null) {
            logger.warn("Time field of record valueField is not set");
            return Paths.get("unknown_date." + outputFileExtension);
        }

        String finalSuffix;
        if(suffix == 0) {
            finalSuffix = "";
        } else {
            finalSuffix = "_" + suffix;
        }

        // Make a timestamped filename YYYYMMDD_HH00.json
        String hourlyTimestamp = createHourTimestamp(date);
        return Paths.get(hourlyTimestamp + "00" + finalSuffix +"." + outputFileExtension);
    }

    public static String createHourTimestamp(Date date) {
        if (date == null) {
            return "unknown_date";
        }

        return FILE_DATE_FORMAT.format(date);
    }

    public static Date getDate(GenericRecord keyField, GenericRecord valueField) {
        Field timeField = valueField.getSchema().getField("time");
        if (timeField != null) {
            double time = (Double) valueField.get(timeField.pos());
            // Convert from millis to date and apply dateFormat
            return new Date((long) (time * 1000d));
        }

        // WindowedKey
        timeField = keyField.getSchema().getField("start");
        if (timeField == null) {
            return null;
        }
        long time = (Long) keyField.get("start");
        return new Date(time);
    }

    public static class Builder {
        private boolean useGzip;
        private boolean doDeduplicate;
        private String hdfsUri;
        private String outputPath;
        private String format;

        public Builder(final String uri, final String outputPath) {
            this.hdfsUri = uri;
            this.outputPath = outputPath;
        }

        public Builder useGzip(final boolean gzip) {
            this.useGzip = gzip;
            return this;
        }

        public Builder doDeuplicate(final boolean dedup) {
            this.doDeduplicate = dedup;
            return this;
        }

        public Builder format(final String format) {
            this.format = format;
            return this;
        }

        public RestructureAvroRecords build() {
            return new RestructureAvroRecords(hdfsUri, outputPath, useGzip, doDeduplicate, format);
        }

    }
}
