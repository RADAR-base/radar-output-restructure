package org.radarcns.hdfs.util;

import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Functional wrapper around a value. Do not nest {@link #read(Function)} and
 * {@link #modify(Consumer)} calls, since that may cause a deadlock in some implementations.
 * @param <T> type of value.
 */
public abstract class FunctionalValue<T> {
    protected final T value;

    protected FunctionalValue(T value) {
        this.value = value;
    }

    /**
     * Read a value and apply a function on it for a result. Calls to this method may not modify
     * the given value but are allowed to have other side-effects.
     * @param function read-only function to apply.
     * @param <V> type of output.
     * @return value computed by given function.
     */
    public abstract <V> V read(Function<T, ? extends V> function);

    /**
     * Modify a value in given consumer. Calls to this method are allowed to have side-effects.
     * @param consumer modifying consumer.
     */
    public abstract void modify(Consumer<? super T> consumer);

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ReadOnlyFunctionalValue<?> that = (ReadOnlyFunctionalValue<?>) o;
        return read(v -> that.read(v::equals));
    }

    @Override
    public int hashCode() {
        return read(Object::hashCode);
    }

}
