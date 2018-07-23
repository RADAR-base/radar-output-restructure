package org.radarcns.hdfs.util;

import com.beust.jcommander.Parameter;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Value protected by a read-write lock.
 * @param <T> type of value.
 */
public class LockedFunctionalValue<T> implements FunctionalValue<T> {
    private final T value;
    private final Lock readLock;
    private final Lock writeLock;

    public LockedFunctionalValue(T initialValue) {
        this.value = initialValue;
        ReadWriteLock lock = new ReentrantReadWriteLock();
        this.readLock = lock.readLock();
        this.writeLock = lock.writeLock();
    }

    @Override
    public <V> V read(Function<T, ? extends V> function) {
        try {
            readLock.lock();
            return function.apply(value);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void modify(Consumer<? super T> consumer) {
        try {
            writeLock.lock();
            consumer.accept(value);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LockedFunctionalValue<?> that = (LockedFunctionalValue<?>) o;
        return read(v -> that.read(v::equals));
    }

    @Override
    public int hashCode() {
        return read(Object::hashCode);
    }
}
