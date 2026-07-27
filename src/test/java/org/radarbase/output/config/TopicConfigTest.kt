package org.radarbase.output.config

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.radarbase.output.path.RecordPathFactory.Companion.projectIdFrom

class TopicConfigTest {
    @Test
    fun projectIdFromCombinedRecord() {
        val schema = Schema.Parser().parse(
            """
            {
              "type": "record",
              "name": "Record",
              "fields": [
                {
                  "name": "key",
                  "type": {
                    "type": "record",
                    "name": "Key",
                    "fields": [
                      { "name": "projectId", "type": "string" },
                      { "name": "userId", "type": "string" },
                      { "name": "sourceId", "type": "string" }
                    ]
                  }
                },
                { "name": "value", "type": { "type": "record", "name": "Value", "fields": [] } }
              ]
            }
            """.trimIndent(),
        )
        val record = GenericData.Record(schema)
        val key = GenericData.Record(schema.getField("key").schema())
        key.put("projectId", "RECURRENT-GB")
        key.put("userId", "user")
        key.put("sourceId", "source")
        record.put("key", key)
        record.put("value", GenericData.Record(schema.getField("value").schema()))

        assertEquals("RECURRENT-GB", projectIdFrom(record))
    }
}
