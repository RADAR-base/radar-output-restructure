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

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Value protected by a read-write lock.
 * @param <T> type of value.
 */
public class LockedFunctionalValue<T> extends FunctionalValue<T> {
    private final Lock readLock;
    private final Lock writeLock;

    public LockedFunctionalValue(T initialValue) {
        super(initialValue);
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
}
