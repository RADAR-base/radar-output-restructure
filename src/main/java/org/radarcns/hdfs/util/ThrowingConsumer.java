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
