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

package org.radarcns.hdfs.accounting;

/** POJO class for storing offsets. */
public class OffsetRange {
    private final TopicPartition topicPartition;
    private final long offsetFrom;
    private final long offsetTo;

    public static OffsetRange parseFilename(String filename) throws NumberFormatException, IndexOutOfBoundsException {
        String[] fileNameParts = filename.split("[+.]");

        return new OffsetRange(
                fileNameParts[0],
                Integer.parseInt(fileNameParts[1]),
                Long.parseLong(fileNameParts[2]),
                Long.parseLong(fileNameParts[3]));
    }

    /** Full constructor. */
    public OffsetRange(String topic, int partition, long offsetFrom, long offsetTo) {
        this(new TopicPartition(topic, partition), offsetFrom, offsetTo);
    }

    public OffsetRange(TopicPartition topicPartition, long offsetFrom, long offsetTo) {
        this.topicPartition = topicPartition;
        this.offsetFrom = offsetFrom;
        this.offsetTo = offsetTo;
    }

    public String getTopic() {
        return topicPartition.topic;
    }

    public int getPartition() {
        return topicPartition.partition;
    }

    public long getOffsetFrom() {
        return offsetFrom;
    }

    public long getOffsetTo() {
        return offsetTo;
    }

    public OffsetRange createSingleOffset(int index) {
        if (index < 0 || index > offsetTo - offsetFrom) {
            throw new IndexOutOfBoundsException("Index " + index + " does not reference offsets "
                    + offsetFrom + " to " + offsetTo);
        }

        long singleOffset = offsetFrom + index;
        return new OffsetRange(topicPartition, singleOffset, singleOffset);
    }

    @Override
    public String toString() {
        return topicPartition.topic + '+' + topicPartition.partition + '+' + offsetFrom + '+' + offsetTo;
    }

    @Override
    public int hashCode() {
        int result = topicPartition.hashCode();
        result = 31 * result + (int) (offsetFrom ^ (offsetFrom >>> 32));
        result = 31 * result + (int) (offsetTo ^ (offsetTo >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || !getClass().equals(o.getClass())) {
            return false;
        }
        OffsetRange other = (OffsetRange) o;
        return topicPartition.equals(other.topicPartition)
                && offsetFrom == other.offsetFrom
                && offsetTo == other.offsetTo;
    }

    public TopicPartition getTopicPartition() {
        return topicPartition;
    }

}
