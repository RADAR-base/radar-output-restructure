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
