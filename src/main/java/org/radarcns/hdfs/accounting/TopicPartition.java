package org.radarcns.hdfs.accounting;

import org.jetbrains.annotations.NotNull;

import java.util.Objects;

public final class TopicPartition implements Comparable<TopicPartition> {
    public final String topic;
    public final int partition;

    public TopicPartition(String topic, int partition) {
        this.topic = topic;
        this.partition = partition;
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
        return Objects.hash(topic, partition);
    }

    @Override
    public int compareTo(@NotNull TopicPartition o) {
        int result = topic.compareTo(o.topic);
        if (result != 0) {
            return result;
        }
        return Integer.compare(partition, o.partition);
    }
}
