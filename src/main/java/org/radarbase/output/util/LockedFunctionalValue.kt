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

package org.radarbase.output.util

import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write

/**
 * Value protected by a read-write lock.
 * @param <T> type of value.
</T> */
class LockedFunctionalValue<T>(initialValue: T) : FunctionalValue<T>(initialValue) {
    private val lock = ReentrantReadWriteLock()

    override fun <V> read(function: (T) -> V): V = lock.read {
        function(value)
    }

    override fun modify(consumer: (T) -> Unit) = lock.write {
        consumer(value)
    }
}
