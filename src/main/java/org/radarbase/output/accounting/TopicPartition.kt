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

import java.util.Objects

@Suppress("EqualsOrHashCode")
data class TopicPartition(val topic: String, val partition: Int) : Comparable<TopicPartition> {
    private val hash: Int = Objects.hash(topic, partition)

    override fun hashCode() = hash

    override fun compareTo(other: TopicPartition): Int = comparator.compare(this, other)

    companion object {
        val comparator = compareBy(TopicPartition::topic, TopicPartition::partition)
    }
}