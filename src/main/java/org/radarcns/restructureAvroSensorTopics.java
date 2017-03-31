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

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

public class restructureAvroSensorTopics {

    String outputPath = ".";
    String OUTPUT_FILE_EXTENSION = "json";
    String OFFSETS_FILE_NAME = "offsets.csv";
    Configuration conf = new Configuration();
    SimpleDateFormat dateFormatFileName = new SimpleDateFormat("yyyyMMdd_HH");

    public static void main(String [] args) throws Exception {
        System.out.println("Started!");

        restructureAvroSensorTopics res = new restructureAvroSensorTopics();
        res.setInputWebHdfsURL("webhdfs://radar-test.thehyve.net:50070");
        res.setOutputPath("output3/");

        res.start("/topicAndroidPhoneNew/");
//        restructureAvroSensorTopics.processTopic("/topicE4/android_empatica_e4_inter_beat_interval/partition=0/");
//        restructureAvroSensorTopics.processAvroFile(new Path("/topicE4/android_empatica_e4_inter_beat_interval/partition=0/android_empatica_e4_inter_beat_interval+0+0000031485+0000031488.avro") );
//        restructureAvroSensorTopics.processAvroFile(new Path("/testE4Time/android_phone_acceleration/partition=0/android_phone_acceleration+0+0000590000+0000599999.avro"),"wazaa" );
    }

    public void setInputWebHdfsURL(String fileSystemURL) {
        conf.set("fs.defaultFS",fileSystemURL);
    }

    public void setOutputPath(String path) {
        // Remove trailing backslash
        outputPath = path.replaceAll("/$","");
    }

    public void start(String directoryName) throws IOException {
        // Get files and directories
        Path path = new Path(directoryName);
        FileSystem fs = FileSystem.get(conf);
        RemoteIterator<LocatedFileStatus> files = fs.listLocatedStatus(path);

        // Process the directories topics
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

    public void processTopic(Path topicPath) throws IOException {
        // Get files in this topic directory
        FileSystem fs = FileSystem.get(conf);
        RemoteIterator<LocatedFileStatus> files = fs.listFiles(topicPath, true); // TODO: all partitions or just 'partition=0'?

        String topicName = topicPath.getName();

        int fileCounter = 0;
        while (files.hasNext()) {
            LocatedFileStatus locatedFileStatus = files.next();

            System.out.println(locatedFileStatus.getPath());

            if (locatedFileStatus.isFile())
                this.processAvroFile( locatedFileStatus.getPath(), topicName );
            fileCounter += 1;

            if (fileCounter > 1)
                continue;
        }
    }

    public void processAvroFile(Path filePath, String topicName) throws IOException {
        String fileName = filePath.getName();

        // Skip if extension is not .avro
        if (! FilenameUtils.getExtension(fileName).equals("avro")) {
            System.out.printf("Skipped non avro file: %s\n", filePath.getName());
            return;
        }

        FsInput input = new FsInput(filePath, conf);
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>();

        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(input, datumReader);

        GenericRecord record = null;
        while (dataFileReader.hasNext()) {
            record = dataFileReader.next(record);

            // Get the fields
            this.writeRecord(record, topicName);
        }

        this.writeSeenOffsets(fileName);
    }

    public void writeRecord(GenericRecord record, String topicName) throws IOException {
        GenericRecord keyField = (GenericRecord) record.get("keyField");
        GenericRecord valueField = (GenericRecord) record.get("valueField");

        // Make a timestamped filename YYYYMMDD_HH00.json
        String outputFileName = this.createFilepathFromTimestamp( (Double) valueField.get("time"));

        // Clean user id and create final output pathname
        String userId = keyField.get("userId").toString().replaceAll("\\W+", "");
        String dirName = this.outputPath + "/" +  userId + "/" + topicName;

        // Write data
        String data = record.toString(); // TODO: check whether this indeed always creates valid JSON
        this.appendToFile(dirName, outputFileName, data);
    }

    public String createFilepathFromTimestamp(Double time) {
        // Send all output to the Appendable object sb
        StringBuilder sb = new StringBuilder();
        Formatter formatter = new Formatter(sb, Locale.US);

        // In millis
        Date date = new Date( time.longValue() * 1000 );

        formatter.format("%s00.%s", dateFormatFileName.format(date), OUTPUT_FILE_EXTENSION);
        return sb.toString();
    }

    public void appendToFile(String directoryName, String fileName, String data) {
        File directory = new File(directoryName);
        if (! directory.exists()){
            directory.mkdirs();
            System.out.printf("Created directory: %s\n", directory.getAbsolutePath());
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
            System.out.println("Could not split filename to the commit offsets.");
            return;
        } catch (NumberFormatException e) {
            System.out.println("Could not convert offsets to integers.");
            return;
        }

        String data = String.join(",", topicName, partition, fromOffset.toString(), toOffset.toString());

        try(FileWriter fw = new FileWriter(outputPath + "/" + OFFSETS_FILE_NAME, true);
            BufferedWriter bw = new BufferedWriter(fw);
            PrintWriter out = new PrintWriter(bw))
        {
            out.println(data);
        } catch (IOException e) {
            // TODO: exception handling
            e.printStackTrace();
        }
    }
}