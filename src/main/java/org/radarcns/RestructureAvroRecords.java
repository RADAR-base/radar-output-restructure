package org.radarcns;

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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

public class RestructureAvroRecords {
    private static final Logger logger = LogManager.getLogger(RestructureAvroRecords.class);

    private final String OUTPUT_FILE_EXTENSION = "json";
    private final String OFFSETS_FILE_NAME = "offsets.csv";
    private final String BINS_FILE_NAME = "bins.csv";

    private String outputPath = ".";
    private String offsetsPath = outputPath + "/" + OFFSETS_FILE_NAME;
    private Set<String> seenFiles = new HashSet<>();
    private Frequency bins = new Frequency();

    private Configuration conf = new Configuration();
    private final static SimpleDateFormat dateFormatFileName = new SimpleDateFormat("yyyyMMdd_HH");

    private int processedFileCount;
    private int processedRecordsCount;

    public static void main(String [] args) throws Exception {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        logger.info(dateFormat.format(new Date()));
        logger.info("Starting...");
        logger.info("In:  " + args[0] + args[1]);
        logger.info("Out: " + args[2]);

        long time1 = System.currentTimeMillis();

        RestructureAvroRecords restr = new RestructureAvroRecords(args[0], args[2]);
        restr.start(args[1]);

        logger.info(String.format("Processed %d files and %,d records", restr.getProcessedFileCount(), restr.getProcessedRecordsCount()));
        logger.info(String.format("Time taken: %.2f seconds",(System.currentTimeMillis() - time1)/1000d));

//        restr.processTopic(new Path("/topicE4/android_empatica_e4_temperature/"));
//        restr.processFile(new Path("/testE4Time/android_phone_acceleration/partition=0/android_phone_acceleration+0+0000590000+0000599999.avro"),"wazaa" );
    }

    public RestructureAvroRecords(String inputPath, String outputPath) {
        this.setInputWebHdfsURL(inputPath);
        this.setOutputPath(outputPath);
    }

    public void setInputWebHdfsURL(String fileSystemURL) {
        conf.set("fs.defaultFS", fileSystemURL);
    }

    public void setOutputPath(String path) {
        // Remove trailing backslash
        outputPath = path.replaceAll("/$","");
        offsetsPath = outputPath + "/" + OFFSETS_FILE_NAME;
        bins.setBinFilePath(outputPath + "/" + BINS_FILE_NAME);
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

        // Load seen offsets from file
        readSeenOffsets();

        // Process the directories topics
        processedFileCount = 0;
        while (files.hasNext()) {
            LocatedFileStatus locatedFileStatus = files.next();
            Path filePath = locatedFileStatus.getPath();

            if (filePath.toString().contains("+tmp")) {
                continue;
            }

            if (locatedFileStatus.isDirectory()) {
                processTopic(filePath);
            }
        }
    }

    private void processTopic(Path topicPath) throws IOException {
        // Get files in this topic directory
        FileSystem fs = FileSystem.get(conf);
        RemoteIterator<LocatedFileStatus> files = fs.listFiles(topicPath, true); // TODO: all partitions or just 'partition=0'?

        String topicName = topicPath.getName();

        while (files.hasNext()) {
            LocatedFileStatus locatedFileStatus = files.next();

            if (locatedFileStatus.isFile())
                this.processFile(locatedFileStatus.getPath(), topicName);
        }
    }

    private void processFile(Path filePath, String topicName) throws IOException {
        String fileName = filePath.getName();

        // Skip if extension is not .avro
        if (! FilenameUtils.getExtension(fileName).equals("avro")) {
            logger.info("Skipped non-avro file: " + fileName);
            return;
        }

        // Skip already processed avro files
        if (seenFiles.contains(fileName)) {
            return;
        }

        logger.info(filePath);

        // Read and parse avro file
        FsInput input = new FsInput(filePath, conf);
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(input, datumReader);

        GenericRecord record = null;
        while (dataFileReader.hasNext()) {
            record = dataFileReader.next(record);

            // Get the fields
            this.writeRecord(record, topicName);
        }

        // Write which file has been processed and update bins
        this.writeSeenOffsets(fileName);
        bins.writeBins();
        processedFileCount++;
    }

    private void writeRecord(GenericRecord record, String topicName) throws IOException {
        GenericRecord keyField = (GenericRecord) record.get("keyField");
        GenericRecord valueField = (GenericRecord) record.get("valueField");

        // Make a timestamped filename YYYYMMDD_HH00.json
        String hourlyTimestamp = createHourTimestamp( (Double) valueField.get("time"));
        String outputFileName = hourlyTimestamp + "00." + OUTPUT_FILE_EXTENSION;

        // Clean user id and create final output pathname
        String userId = keyField.get("userId").toString().replaceAll("\\W+", "");
        String dirName = this.outputPath + "/" +  userId + "/" + topicName;

        // Write data
        String data = record.toString(); // TODO: check whether this indeed always creates valid JSON
        this.appendToFile(dirName, outputFileName, data);

        // Count data (binned and total)
        bins.addToBin(topicName, keyField.get("sourceId").toString(), (Double) valueField.get("time"));
        processedRecordsCount++;
    }

    public static String createHourTimestamp(Double time) {
        // Convert from millis to date and apply dateFormat
        Date date = new Date( time.longValue() * 1000 );
        return dateFormatFileName.format(date);
    }

    private void appendToFile(String directoryName, String fileName, String data) {
        File directory = new File(directoryName);
        if (! directory.exists()){
            if (directory.mkdirs())
                logger.info("Created directory: " + directory.getAbsolutePath());
            else
                logger.warn("FAILED to create directory: " + directory.getAbsolutePath());
        }

        String filePath = directoryName + "/" + fileName;

        // Buffered writer to prevent opening and closing the same file many times
        try(FileWriter fw = new FileWriter(filePath, true);
            BufferedWriter bw = new BufferedWriter(fw);
            PrintWriter out = new PrintWriter(bw))
        {
            out.println(data);
        } catch (IOException e) {
            // TODO: exception handling
            e.printStackTrace();
        }
    }

    private void writeSeenOffsets(String fileName) {
        String[] fileNameParts = fileName.split("[+.]");

        String topicName, partition;
        Integer fromOffset, toOffset;
        try {
            topicName = fileNameParts[0];
            partition = fileNameParts[1];
            fromOffset = Integer.valueOf( fileNameParts[2] );
            toOffset = Integer.valueOf( fileNameParts[3] );
        } catch (IndexOutOfBoundsException e) {
            logger.warn("Could not split filename to the commit offsets.");
            return;
        } catch (NumberFormatException e) {
            logger.warn("Could not convert offsets to integers.");
            return;
        }

        String data = String.join(",", fileName, topicName, partition, fromOffset.toString(), toOffset.toString());

        try(FileWriter fw = new FileWriter(offsetsPath, true);
            BufferedWriter bw = new BufferedWriter(fw);
            PrintWriter out = new PrintWriter(bw))
        {
            out.println(data);
        } catch (IOException e) {
            // TODO: exception handling
            e.printStackTrace();
        }
    }

    private void readSeenOffsets() {
        try (FileReader fr = new FileReader(offsetsPath);
             BufferedReader br = new BufferedReader(fr))
        {
            // Read in all file names from csv
            String line;
            while ( (line = br.readLine()) != null ) {
                String[] columns = line.split(",");
                seenFiles.add(columns[0]);
            }
        } catch (IOException e) {
            logger.info("Offsets file does not exist yet, will be created.");
        }
    }
}