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

/**
 * Functional wrapper around a value. Do not nest [.read] and
 * [.modify] calls, since that may cause a deadlock in some implementations.
 * @param <T> type of value.
</T> */
abstract class FunctionalValue<T> protected constructor(protected val value: T) {

    /**
     * Read a value and apply a function on it for a result. Calls to this method may not modify
     * the given value but are allowed to have other side-effects.
     * @param function read-only function to apply.
     * @param <V> type of output.
     * @return value computed by given function.
    </V> */
    abstract fun <V> read(function: (T) -> V): V

    /**
     * Modify a value in given consumer. Calls to this method are allowed to have side-effects.
     * @param consumer modifying consumer.
     */
    abstract fun modify(consumer: (T) -> Unit)

    override fun toString(): String {
        return read { it.toString() }
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as ReadOnlyFunctionalValue<*>
        return read { v -> other.read { v == it } }
    }

    override fun hashCode(): Int {
        return read { it.hashCode() }
    }

}
