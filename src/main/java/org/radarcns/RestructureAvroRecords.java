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

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RestructureAvroRecords {
    private static final Logger logger = LoggerFactory.getLogger(RestructureAvroRecords.class);

    private final String outputFileExtension;
    private static final String OFFSETS_FILE_NAME = "offsets.csv";
    private static final String BINS_FILE_NAME = "bins.csv";
    private static final SimpleDateFormat FILE_DATE_FORMAT = new SimpleDateFormat("yyyyMMdd_HH");

    static {
        FILE_DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    private final RecordConverterFactory converterFactory;

    private File outputPath;
    private File offsetsPath;
    private Frequency bins;

    private final Configuration conf = new Configuration();

    private long processedFileCount;
    private long processedRecordsCount;
    private static final boolean USE_GZIP = "gzip".equalsIgnoreCase(System.getProperty("org.radarcns.compression"));

    public static void main(String [] args) throws Exception {
        if (args.length != 3) {
            System.out.println("Usage: hadoop jar restructurehdfs-all-0.2.jar <webhdfs_url> <hdfs_root_directory> <output_folder>");
            System.exit(1);
        }

        logger.info(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
        logger.info("Starting...");
        logger.info("In:  " + args[0] + args[1]);
        logger.info("Out: " + args[2]);

        long time1 = System.currentTimeMillis();

        RestructureAvroRecords restr = new RestructureAvroRecords(args[0], args[2]);
        try {
            restr.start(args[1]);
        } catch (IOException ex) {
            logger.error("Processing failed", ex);
        }

        logger.info("Processed {} files and {} records", restr.getProcessedFileCount(), restr.getProcessedRecordsCount());
        logger.info("Time taken: {} seconds", (System.currentTimeMillis() - time1)/1000d);
    }

    public RestructureAvroRecords(String inputPath, String outputPath) {
        this.setInputWebHdfsURL(inputPath);
        this.setOutputPath(outputPath);

        String extension;
        if (System.getProperty("org.radarcns.format", "csv").equalsIgnoreCase("json")) {
            logger.info("Writing output files in JSON format");
            converterFactory = JsonAvroConverter.getFactory();
            extension = "json";
        } else {
            logger.info("Writing output files in CSV format");
            converterFactory = CsvAvroConverter.getFactory();
            extension = "csv";
        }
        if (USE_GZIP) {
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
        outputPath = new File(path.replaceAll("/$",""));
        offsetsPath = new File(outputPath, OFFSETS_FILE_NAME);
        bins = Frequency.read(new File(outputPath, BINS_FILE_NAME));
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


        try (OffsetRangeFile offsets = new OffsetRangeFile(offsetsPath)) {
            OffsetRangeSet seenFiles;
            try {
                seenFiles = offsets.read();
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
                try (FileCacheStore cache = new FileCacheStore(converterFactory, 100, USE_GZIP)) {
                    for (Path filePath : entry.getValue()) {
                        this.processFile(filePath, entry.getKey(), cache, offsets);
                        progressBar.update(++processedFileCount);
                    }
                }
            }
        }
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

        OffsetRange range = OffsetRange.parse(fileName);
        // Skip already processed avro files
        if (seenFiles.contains(range)) {
            return null;
        }

        return filePath.getParent().getParent().getName();
    }

    private void processFile(Path filePath, String topicName, FileCacheStore cache,
            OffsetRangeFile offsets) throws IOException {
        logger.debug("Reading {}", filePath);

        // Read and parse avro file
        FsInput input = new FsInput(filePath, conf);
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(input,
                new GenericDatumReader<>());

        GenericRecord record = null;
        while (dataFileReader.hasNext()) {
            record = dataFileReader.next(record);

            // Get the fields
            this.writeRecord(record, topicName, cache);
        }

        // Write which file has been processed and update bins
        try {
            OffsetRange range = OffsetRange.parse(filePath.getName());
            offsets.write(range);
            bins.write();
        } catch (IOException ex) {
            logger.warn("Failed to update status. Continuing processing.", ex);
        }
    }

    private void writeRecord(GenericRecord record, String topicName, FileCacheStore cache)
            throws IOException {
        GenericRecord keyField = (GenericRecord) record.get("key");
        GenericRecord valueField = (GenericRecord) record.get("value");

        if (keyField == null || valueField == null) {
            logger.error("Failed to process {}", record);
            throw new IOException("Failed to process " + record + "; no key or value");
        }

        Field timeField = valueField.getSchema().getField("time");
        String outputFileName = createFilename(valueField, timeField);

        // Clean user id and create final output pathname
        String userId = keyField.get("userId").toString().replaceAll("\\W+", "");
        File userDir = new File(this.outputPath, userId);
        File userTopicDir = new File(userDir, topicName);
        File outputFile = new File(userTopicDir, outputFileName);

        // Write data
        cache.writeRecord(outputFile, record);

        // Count data (binned and total)
        bins.add(topicName, keyField.get("sourceId").toString(), valueField, timeField);
        processedRecordsCount++;
    }

    private String createFilename(GenericRecord valueField, Field timeField) {
        if (timeField == null) {
            logger.warn("Time field of record valueField " + valueField + " is not set");
            return "unknown." + outputFileExtension;
        }
        // Make a timestamped filename YYYYMMDD_HH00.json
        String hourlyTimestamp = createHourTimestamp(valueField, timeField);
        return hourlyTimestamp + "00." + outputFileExtension;
    }

    public static String createHourTimestamp(GenericRecord valueField, Field timeField) {
        if (timeField == null) {
            return "unknown";
        }

        double time = (Double) valueField.get(timeField.pos());
        // Convert from millis to date and apply dateFormat
        Date date = new Date((long) (time * 1000d));
        return FILE_DATE_FORMAT.format(date);
    }

}
