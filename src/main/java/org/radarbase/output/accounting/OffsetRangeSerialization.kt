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

import java.io.Closeable
import java.io.Flushable
import java.nio.file.Path

/**
 * Accesses a OffsetRange file using the CSV format. On construction, this will create the file if
 * not present.
 */
interface OffsetRangeSerialization : Closeable, Flushable {
    val offsets: OffsetRangeSet

    fun add(range: OffsetRange) = offsets.add(range)

    fun addAll(rangeSet: OffsetRangeSet) = offsets.addAll(rangeSet)

    fun triggerWrite()

    interface Reader {
        fun read(path: Path): OffsetRangeSerialization
    }
}
