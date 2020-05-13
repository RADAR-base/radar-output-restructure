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

package org.radarbase.output

import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericRecordBuilder
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.radarbase.output.path.ObservationKeyPathFactory
import org.radarbase.output.path.RecordPathFactory

class RadarHdfsRestructureTest {
    @Test
    fun createHourTimestamp() {
        val factory = ObservationKeyPathFactory()

        val currentTime: Long = 1493711175  // Tue May  2 07:46:15 UTC 2017
        val startTime = (currentTime - 3600) * 1000L

        val keySchema = SchemaBuilder.record("value").fields()
                .name("start").type("long").noDefault()
                .endRecord()
        val keyField = GenericRecordBuilder(keySchema)
                .set("start", startTime).build()

        val valueSchema1 = SchemaBuilder.record("value").fields()
                .name("time").type("double").noDefault()
                .endRecord()
        val valueField1 = GenericRecordBuilder(valueSchema1)
                .set("time", currentTime.toDouble()).build()

        var date = RecordPathFactory.getDate(keyField, valueField1)
        var result = factory.getTimeBin(date)

        assertEquals("20170502_0700", result)

        val valueSchema2 = SchemaBuilder.record("value").fields()
                .name("a").type("double").noDefault()
                .endRecord()
        val valueField2 = GenericRecordBuilder(valueSchema2)
                .set("a", 0.1).build()
        date = RecordPathFactory.getDate(keyField, valueField2)
        result = factory.getTimeBin(date)
        assertEquals("20170502_0600", result)
    }
}
