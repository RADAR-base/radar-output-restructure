package org.radarcns.hdfs.util;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

public class Timer {
    private final ConcurrentMap<Category, LongAdder> times;
    private static final Timer instance = new Timer();
    private volatile boolean isEnabled;

    public static Timer getInstance() {
        return instance;
    }

    private Timer() {
        this.times = new ConcurrentHashMap<>();
    }

    public void add(String type, long nanoTime) {
        if (isEnabled) {
            Category cat = new Category(type);
            times.computeIfAbsent(cat, c -> new LongAdder()).add(nanoTime);
        }
    }

    public void setEnabled(boolean enabled) {
        isEnabled = enabled;
    }

    @Override
    public String toString() {
        if (!isEnabled) {
            return "Timings: disabled";
        }
        StringBuilder builder = new StringBuilder();
        builder.append("Timings:");

        ConcurrentMap<String, List<Map.Entry<Category, LongAdder>>> timesByType = this.times
                .entrySet().stream()
                .collect(Collectors.groupingByConcurrent(c -> c.getKey().type));

        for (List<Map.Entry<Category, LongAdder>> value : timesByType.values()) {
            long time = value.stream().mapToLong(e -> e.getValue().sum()).sum();
            builder.append("\n\t");
            builder.append(value.get(0).getKey().type);
            builder.append(" - time: ");
            int seconds = (int)(time / 1_000_000_000L);
            int millis = (int) (time / 1_000_000L);
            ProgressBar.formatTime(builder, seconds);
            builder.append('.');
            if (millis < 100) builder.append('0');
            if (millis < 10) builder.append('0');
            builder.append(millis);
            builder.append(" - threads: ");
            builder.append(value.size());
        }

        return builder.toString();
    }

    private static class Category {
        private final String type;
        private final String thread;

        Category(@Nonnull String type) {
            this.type = type;
            this.thread = Thread.currentThread().getName();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Category category = (Category) o;
            return Objects.equals(type, category.type) &&
                    Objects.equals(thread, category.thread);
        }

        @Override
        public int hashCode() {
            return Objects.hash(type, thread);
        }
    }
}
