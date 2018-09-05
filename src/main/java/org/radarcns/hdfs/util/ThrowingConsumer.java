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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/** Consumer that throws IOException. */
@FunctionalInterface
public interface ThrowingConsumer<T> {
    void accept(T t) throws IOException;

    /** Regularizes throwing consumer by catching the exception. */
    static <T> Consumer<T> tryCatch(ThrowingConsumer<T> consumer, BiConsumer<T, IOException> catchClause) {
        return t -> {
            try {
                consumer.accept(t);
            } catch (IOException ex) {
                catchClause.accept(t, ex);
            }
        };
    }

    /** Regularizes throwing consumer by rethrowing IOException as UncheckedIOException. */
    static <T> Consumer<T> tryCatch(ThrowingConsumer<T> consumer, String uncheckedIo) {
        return tryCatch(consumer, (t, ex) -> {
            throw new UncheckedIOException(uncheckedIo, ex);
        });
    }
}
