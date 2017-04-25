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
import java.util.Date;
import org.apache.avro.Schema.Field;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.FsInput;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.radarcns.util.CsvAvroConverter;
import org.radarcns.util.FileCache;
import org.radarcns.util.JsonAvroConverter;
import org.radarcns.util.RecordConverterFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RestructureAvroRecords {
    private static final Logger logger = LoggerFactory.getLogger(RestructureAvroRecords.class);

    private final String outputFileExtension;
    private static final String OFFSETS_FILE_NAME = "offsets.csv";
    private static final String BINS_FILE_NAME = "bins.csv";
    private static final SimpleDateFormat FILE_DATE_FORMAT = new SimpleDateFormat("yyyyMMdd_HH");

    private final RecordConverterFactory converterFactory;

    private File outputPath;
    private File offsetsPath;
    private OffsetRangeSet seenFiles;
    private Frequency bins;

    private final Configuration conf = new Configuration();

    private int processedFileCount;
    private int processedRecordsCount;

    public static void main(String [] args) throws Exception {
        if (args.length != 3) {
            System.out.println("Usage: hadoop jar restructurehdfs-all-0.1.0.jar <webhdfs_url> <hdfs_topic> <output_folder>");
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

        if (System.getProperty("org.radarcns.format", "csv").equalsIgnoreCase("json")) {
            converterFactory = JsonAvroConverter.getFactory();
            outputFileExtension = "json";
        } else {
            converterFactory = CsvAvroConverter.getFactory();
            outputFileExtension = "csv";
        }
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

    public int getProcessedFileCount() {
        return processedFileCount;
    }

    public int getProcessedRecordsCount() {
        return processedRecordsCount;
    }

    public void start(String directoryName) throws IOException {
        // Get files and directories
        Path path = new Path(directoryName);
        FileSystem fs = FileSystem.get(conf);
        RemoteIterator<LocatedFileStatus> files = fs.listLocatedStatus(path);

        try (OffsetRangeFile offsets = new OffsetRangeFile(offsetsPath)) {
            try {
                seenFiles = offsets.read();
            } catch (IOException ex) {
                logger.error("Error reading offsets file. Processing all offsets.");
                seenFiles = new OffsetRangeSet();
            }
            // Process the directories topics
            processedFileCount = 0;
            while (files.hasNext()) {
                LocatedFileStatus locatedFileStatus = files.next();
                Path filePath = locatedFileStatus.getPath();

                if (filePath.toString().contains("+tmp")) {
                    continue;
                }

                if (locatedFileStatus.isDirectory()) {
                    processTopic(filePath, converterFactory, offsets);
                }
            }
        }
    }

    private void processTopic(Path topicPath, RecordConverterFactory converterFactory,
            OffsetRangeFile offsets) throws IOException {
        // Get files in this topic directory
        FileSystem fs = FileSystem.get(conf);
        RemoteIterator<LocatedFileStatus> files = fs.listFiles(topicPath, true);

        String topicName = topicPath.getName();

        try (FileCache cache = new FileCache(converterFactory, 100)) {
            while (files.hasNext()) {
                LocatedFileStatus locatedFileStatus = files.next();

                if (locatedFileStatus.isFile()) {
                    this.processFile(locatedFileStatus.getPath(), topicName, cache, offsets);
                }
            }
        }
    }

    private void processFile(Path filePath, String topicName, FileCache cache,
            OffsetRangeFile offsets) throws IOException {
        String fileName = filePath.getName();

        // Skip if extension is not .avro
        if (!FilenameUtils.getExtension(fileName).equals("avro")) {
            logger.info("Skipped non-avro file: {}", fileName);
            return;
        }

        OffsetRange range = OffsetRange.parse(fileName);
        // Skip already processed avro files
        if (seenFiles.contains(range)) {
            return;
        }

        logger.info("{}", filePath);

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
            offsets.write(range);
            bins.write();
        } catch (IOException ex) {
            logger.warn("Failed to update status. Continuing processing.", ex);
        }
        processedFileCount++;
    }

    private void writeRecord(GenericRecord record, String topicName, FileCache cache)
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
