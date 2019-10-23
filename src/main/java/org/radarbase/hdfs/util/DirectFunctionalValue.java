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

package org.radarbase.hdfs.util;

import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Value protected by a read-write lock.
 * @param <T> type of value.
 */
public class DirectFunctionalValue<T> extends FunctionalValue<T> {
    public DirectFunctionalValue(T initialValue) {
        super(initialValue);
    }

    @Override
    public <V> V read(Function<T, ? extends V> function) {
        return function.apply(value);
    }

    @Override
    public void modify(Consumer<? super T> consumer) {
        consumer.accept(value);
    }
}
