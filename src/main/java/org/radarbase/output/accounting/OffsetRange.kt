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

import com.fasterxml.jackson.annotation.JsonIgnore

/** POJO class for storing offsets.  */
data class OffsetRange(
        val topicPartition: TopicPartition,
        val offsetFrom: Long,
        val offsetTo: Long) {

    @JsonIgnore
    val topic: String = topicPartition.topic
    @JsonIgnore
    val partition: Int = topicPartition.partition

    /** Full constructor.  */
    constructor(topic: String, partition: Int, offsetFrom: Long, offsetTo: Long) : this(TopicPartition(topic, partition), offsetFrom, offsetTo) {}

    override fun toString(): String = "$topic+$partition+$offsetFrom+$offsetTo"

    companion object {
        @Throws(NumberFormatException::class, IndexOutOfBoundsException::class)
        fun parseFilename(filename: String): OffsetRange {
            val fileNameParts = filename.split("[+.]".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()

            return OffsetRange(
                    fileNameParts[0],
                    Integer.parseInt(fileNameParts[1]),
                    java.lang.Long.parseLong(fileNameParts[2]),
                    java.lang.Long.parseLong(fileNameParts[3]))
        }
    }
}
