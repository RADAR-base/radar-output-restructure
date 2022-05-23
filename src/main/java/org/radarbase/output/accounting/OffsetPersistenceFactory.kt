/*
 * Copyright 2017 The Hyve
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

package org.radarbase.output.accounting

import kotlinx.coroutines.CoroutineScope
import org.radarbase.output.util.SuspendedCloseable
import java.nio.file.Path

/**
 * Accesses a OffsetRange file using the CSV format. On construction, this will create the file if
 * not present.
 */
interface OffsetPersistenceFactory {
    /**
     * Read offsets from the persistence store. On error, this will return null.
     */
    suspend fun read(path: Path): OffsetRangeSet?

    /**
     * Create a writer to write offsets to the persistence store.
     * Always close the writer after use.
     */
    fun writer(scope: CoroutineScope, path: Path, startSet: OffsetRangeSet? = null): Writer

    /** Offset Writer to given persistence type. */
    interface Writer : SuspendedCloseable {
        /** Current offsets. */
        val offsets: OffsetRangeSet

        /**
         * Add a single offset range to the writer.
         */
        fun add(range: TopicPartitionOffsetRange) = offsets.add(range)

        /**
         * Add all elements in given offset range set to the writer.
         */
        fun addAll(rangeSet: OffsetRangeSet) = offsets.addAll(rangeSet)

        /**
         * Trigger an asynchronous write operation. If this is called multiple times before the
         * operation is executed, the operation will be executed only once.
         */
        suspend fun triggerWrite()

        suspend fun flush()
    }
}
