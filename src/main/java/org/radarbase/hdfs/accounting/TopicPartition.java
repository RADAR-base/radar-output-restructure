/*
 * Copyright 2018 The Hyve
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

package org.radarbase.hdfs.accounting;

import java.util.Objects;
import javax.annotation.Nonnull;

public final class TopicPartition implements Comparable<TopicPartition> {
    public final String topic;
    public final int partition;
    private final int hash;

    public TopicPartition(String topic, int partition) {
        this.topic = topic;
        this.partition = partition;
        this.hash = Objects.hash(topic, partition);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TopicPartition that = (TopicPartition) o;
        return partition == that.partition &&
                Objects.equals(topic, that.topic);
    }

    @Override
    public int hashCode() {
        return hash;
    }

    @Override
    public int compareTo(@Nonnull TopicPartition o) {
        int result = topic.compareTo(o.topic);
        if (result != 0) {
            return result;
        }
        return Integer.compare(partition, o.partition);
    }
}
