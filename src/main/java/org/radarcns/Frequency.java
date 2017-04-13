package org.radarcns;

import org.apache.commons.collections.MapIterator;
import org.apache.commons.collections.keyvalue.MultiKey;
import org.apache.commons.collections.map.MultiKeyMap;

import java.io.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Frequency {
    private static final Logger logger = LoggerFactory.getLogger(Frequency.class);

    private MultiKeyMap bins = new MultiKeyMap();
    private File binFilePath;

    public void setBinFilePath(File binFilePath) {
        this.binFilePath = binFilePath;
    }

    public MultiKeyMap getBins() {
        return bins;
    }

    public void addToBin(String topicName, String id, String timestamp, int countToAdd) {
        Integer count = (Integer) bins.get(topicName, id, timestamp);
        if (count == null) {
            bins.put(topicName, id, timestamp, countToAdd);
        } else {
            bins.put(topicName, id, timestamp, count + countToAdd);
        }
    }

    public void addToBin(String topicName, String id, Double time, int countToAdd) {
        // Hour resolution
        String hourlyTimestamp = RestructureAvroRecords.createHourTimestamp(time);

        addToBin(topicName, id, hourlyTimestamp, countToAdd);
    }

    public void addToBin(String topicName, String id, Double time) {
        addToBin(topicName, id, time, 1);
    }

    public void printBins() {
        MapIterator mapIterator = bins.mapIterator();

        while (mapIterator.hasNext()) {
            MultiKey key = (MultiKey) mapIterator.next();
            Integer value = (Integer) mapIterator.getValue();
            System.out.printf("%s|%s|%s - %d\n", key.getKey(0), key.getKey(1), key.getKey(2), value);
        }
    }

    public void writeBins() {
        // Read bins from file and add to current bins
        // Creates new bins if not existing yet
        addBinsFromFile();

        // Write all bins to csv
        MapIterator mapIterator = bins.mapIterator();
        try(FileWriter fw = new FileWriter(binFilePath, false);
            BufferedWriter bw = new BufferedWriter(fw);
            PrintWriter out = new PrintWriter(bw))
        {
            String header = String.join(",","topic","device","timestamp","count");
            out.println(header);

            while (mapIterator.hasNext()) {
                MultiKey key = (MultiKey) mapIterator.next();
                Integer value = (Integer) mapIterator.getValue();
                String data = String.join(",", key.getKey(0).toString(), key.getKey(1).toString(), key.getKey(2).toString(), value.toString());
                out.println(data);
            }
        } catch (IOException e) {
            // TODO: exception handling
            e.printStackTrace();
        }

        // Reset the map
        bins = new MultiKeyMap();
    }

    private void addBinsFromFile() {
        try (FileReader fr = new FileReader(binFilePath);
             BufferedReader br = new BufferedReader(fr))
        {
            // Read in all lines as multikeymap (key, key, key, value)
            String line;
            br.readLine(); // Skip header
            while ( (line = br.readLine()) != null ) {
                String[] columns = line.split(",");
                this.addToBin(columns[0], columns[1], columns[2], Integer.valueOf(columns[3]));
            }
        } catch (IOException e) {
            logger.warn("Could not read the file with bins. Creating new file when writing.");
        } catch (ArrayIndexOutOfBoundsException e) {
            logger.warn("Unable to parse the contents of the bins file. Skipping reading.");
        }
    }
}
