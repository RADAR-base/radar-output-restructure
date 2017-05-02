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

package org.radarcns;

import static org.junit.Assert.*;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.Test;

public class RestructureAvroRecordsTest {
    @Test
    public void createHourTimestamp() throws Exception {
        long currentTime = 1493711175;  // Tue May  2 07:46:15 UTC 2017

        Schema schema = SchemaBuilder.record("value").fields()
                .name("time").type("double").noDefault()
                .endRecord();
        GenericRecord record = new GenericRecordBuilder(schema).set("time",
                (double)currentTime).build();
        Field timeField = schema.getField("time");
        String result = RestructureAvroRecords.createHourTimestamp(record, timeField);

        assertEquals("20170502_07", result);
    }
}