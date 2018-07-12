package org.radarcns.util;

import java.io.IOException;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

@FunctionalInterface
public interface ThrowingConsumer<T> {
    void accept(T t) throws IOException;

    static <T> Consumer<T> tryCatch(ThrowingConsumer<T> consumer, BiConsumer<T, IOException> catchClause) {
        return t -> {
            try {
                consumer.accept(t);
            } catch (IOException ex) {
                catchClause.accept(t, ex);
            }
        };
    }
}
