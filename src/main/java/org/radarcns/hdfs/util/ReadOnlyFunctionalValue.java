package org.radarcns.hdfs.util;

import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Value protected by a read-write lock.
 * @param <T> type of value.
 */
public class ReadOnlyFunctionalValue<T> extends FunctionalValue<T> {
    public ReadOnlyFunctionalValue(T initialValue) {
        super(initialValue);
    }

    @Override
    public <V> V read(Function<T, ? extends V> function) {
        return function.apply(value);
    }

    @Override
    public void modify(Consumer<? super T> consumer) {
        throw new UnsupportedOperationException();
    }
}
