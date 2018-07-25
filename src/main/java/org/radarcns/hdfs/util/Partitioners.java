package org.radarcns.hdfs.util;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collector;

public final class Partitioners {
    private Partitioners() {
        // utility class
    }

    public static <T, A, R> Collector<T, ?, R> unorderedBatches(int batchSize,
            Collector<List<T>, A, R> downstream) {
        return unorderedBatches(batchSize, t -> 1, downstream);
    }

    // Adapted from https://stackoverflow.com/a/32435407/574082 with custom size
    public static <T, A, R> Collector<T, ?, R> unorderedBatches(int batchSize,
            Function<T, Integer> itemSize,
            Collector<List<T>, A, R> downstream) {
        class Acc {
            private List<T> cur = new ArrayList<>();
            private A acc = downstream.supplier().get();
            private int size = 0;
        }
        BiConsumer<Acc, T> accumulator = (acc, t) -> {
            acc.cur.add(t);
            acc.size += itemSize.apply(t);
            if(acc.size >= batchSize) {
                downstream.accumulator().accept(acc.acc, acc.cur);
                acc.cur = new ArrayList<>();
                acc.size = 0;
            }
        };
        return Collector.of(Acc::new, accumulator,
                (acc1, acc2) -> {
                    acc1.acc = downstream.combiner().apply(acc1.acc, acc2.acc);
                    for(T t : acc2.cur) accumulator.accept(acc1, t);
                    return acc1;
                }, acc -> {
                    if (!acc.cur.isEmpty())
                        downstream.accumulator().accept(acc.acc, acc.cur);
                    return downstream.finisher().apply(acc.acc);
                }, Collector.Characteristics.UNORDERED);
    }
}
