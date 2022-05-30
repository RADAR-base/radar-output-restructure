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

package org.radarbase.output.accounting

import kotlinx.coroutines.CoroutineScope
import org.radarbase.output.util.DirectFunctionalValue
import org.radarbase.output.util.SuspendedCloseable
import org.radarbase.output.util.Timer.time
import java.time.Instant

interface Accountant : SuspendedCloseable {
    val offsets: OffsetRangeSet

    suspend fun initialize(scope: CoroutineScope)
    suspend fun remove(range: TopicPartitionOffsetRange)
    suspend fun process(ledger: Ledger)
    suspend fun flush()

    class Ledger {
        internal val offsets: OffsetRangeSet = OffsetRangeSet { DirectFunctionalValue(it) }

        fun add(transaction: Transaction) = time("accounting.add") {
            offsets.add(transaction.topicPartition, transaction.offset, transaction.lastModified)
        }
    }

    class Transaction(
        val topicPartition: TopicPartition,
        internal var offset: Long,
        internal val lastModified: Instant,
    )
}
