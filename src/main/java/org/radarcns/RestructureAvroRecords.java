package org.radarcns;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
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

    private File outputPath = new File(".");
    private File offsetsPath = new File(outputPath, OFFSETS_FILE_NAME);
    private OffsetRangeSet seenFiles;
    private final Frequency bins = new Frequency();

    private final Configuration conf = new Configuration();
    private final DatumReader<GenericRecord> datumReader;

    private int processedFileCount;
    private int processedRecordsCount;

    public static void main(String [] args) throws Exception {
//        Thread.sleep(120_000);
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

        logger.info("Processed %d files and %,d records", restr.getProcessedFileCount(), restr.getProcessedRecordsCount());
        logger.info("Time taken: %.2f seconds", (System.currentTimeMillis() - time1)/1000d);
    }

    public RestructureAvroRecords(String inputPath, String outputPath) {
        this.setInputWebHdfsURL(inputPath);
        this.setOutputPath(outputPath);
        datumReader = new GenericDatumReader<>();

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
        bins.setBinFilePath(new File(outputPath, BINS_FILE_NAME));
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
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(input, datumReader);

        GenericRecord record = null;
        while (dataFileReader.hasNext()) {
            record = dataFileReader.next(record);

            // Get the fields
            this.writeRecord(record, topicName, cache);
        }

        // Write which file has been processed and update bins
        try {
            offsets.write(range);
            bins.writeBins();
        } catch (IOException ex) {
            logger.warn("Failed to update status. Continuing processing.", ex);
        }
        processedFileCount++;
    }

    private void writeRecord(GenericRecord record, String topicName, FileCache cache)
            throws IOException {
        GenericRecord keyField = (GenericRecord) record.get("keyField");
        GenericRecord valueField = (GenericRecord) record.get("valueField");

        // Make a timestamped filename YYYYMMDD_HH00.json
        String hourlyTimestamp = createHourTimestamp( (Double) valueField.get("time"));
        String outputFileName = hourlyTimestamp + "00." + outputFileExtension;

        // Clean user id and create final output pathname
        String userId = keyField.get("userId").toString().replaceAll("\\W+", "");
        File userDir = new File(this.outputPath, userId);
        File userTopicDir = new File(userDir, topicName);
        File outputFile = new File(userTopicDir, outputFileName);

        // Write data
        cache.writeRecord(outputFile, record);

        // Count data (binned and total)
        bins.addToBin(topicName, keyField.get("sourceId").toString(), (Double) valueField.get("time"));
        processedRecordsCount++;
    }

    public static String createHourTimestamp(Double time) {
        // Convert from millis to date and apply dateFormat
        Date date = new Date( time.longValue() * 1000 );
        return FILE_DATE_FORMAT.format(date);
    }
}
