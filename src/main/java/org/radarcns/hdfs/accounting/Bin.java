package org.radarcns.hdfs.accounting;

import javax.annotation.Nonnull;
import java.util.Objects;

public class Bin {
    private final String topic;
    private final String category;
    private final String time;

    public Bin(@Nonnull String topic, @Nonnull String category, @Nonnull String time) {
        this.topic = topic;
        this.category = category;
        this.time = time;
    }

    public String getTopic() {
        return topic;
    }

    public String getCategory() {
        return category;
    }

    public String getTime() {
        return time;
    }

    @Override
    public String toString() {
        return topic + '|' + category + '|' + time;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Bin bin = (Bin) o;
        return topic.equals(bin.topic) &&
                category.equals(bin.category) &&
                time.equals(bin.time);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topic, category, time);
    }
}
