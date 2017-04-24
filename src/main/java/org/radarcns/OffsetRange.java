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

import java.util.Objects;
import javax.annotation.Nonnull;

/** POJO class for storing offsets. */
public class OffsetRange implements Comparable<OffsetRange> {
    private String topic;
    private int partition;
    private long offsetFrom;
    private long offsetTo;

    public static OffsetRange parse(String filename) throws NumberFormatException, IndexOutOfBoundsException {
        String[] fileNameParts = filename.split("[+.]");

        OffsetRange range = new OffsetRange();
        range.topic = fileNameParts[0];
        range.partition = Integer.parseInt(fileNameParts[1]);
        range.offsetFrom = Long.parseLong(fileNameParts[2]);
        range.offsetTo = Long.parseLong(fileNameParts[3]);
        return range;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(@Nonnull String topic) {
        Objects.requireNonNull(topic);
        this.topic = topic;
    }

    public int getPartition() {
        return partition;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }

    public long getOffsetFrom() {
        return offsetFrom;
    }

    public void setOffsetFrom(long offsetFrom) {
        this.offsetFrom = offsetFrom;
    }

    public long getOffsetTo() {
        return offsetTo;
    }

    public void setOffsetTo(long offsetTo) {
        this.offsetTo = offsetTo;
    }

    @Override
    public String toString() {
        return topic + '+' + partition + '+' + offsetFrom + '+' + offsetTo;
    }

    @Override
    public int hashCode() {
        int result = topic.hashCode();
        result = 31 * result + partition;
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
        return topic.equals(other.topic)
                && partition == other.partition
                && offsetFrom == other.offsetFrom
                && offsetTo == other.offsetTo;
    }

    @Override
    public int compareTo(OffsetRange o) {
        int ret = Long.compare(offsetFrom, o.offsetFrom);
        if (ret != 0) {
            return ret;
        }
        ret = Long.compare(offsetTo, o.offsetTo);
        if (ret != 0) {
            return ret;
        }
        ret = topic.compareTo(o.topic);
        if (ret != 0) {
            return ret;
        }
        return Integer.compare(partition, o.partition);
    }
}
