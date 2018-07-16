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

package org.radarcns.hdfs.data;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.util.MinimalPrettyPrinter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Writes an Avro record to JSON format.
 */
public final class JsonAvroConverter implements RecordConverter {

    public static RecordConverterFactory getFactory() {
        JsonFactory factory = new JsonFactory();
        return (writer, record, writeHeader, reader) -> new JsonAvroConverter(factory, writer);
    }

    private final ObjectWriter jsonWriter;
    private final JsonGenerator generator;

    public JsonAvroConverter(JsonFactory factory, Writer writer) throws IOException {
        generator = factory.createGenerator(writer).setPrettyPrinter(new MinimalPrettyPrinter("\n"));
        jsonWriter = new ObjectMapper(factory).writer();
    }

    @Override
    public boolean writeRecord(GenericRecord record) throws IOException {
        jsonWriter.writeValue(generator, convertRecord(record));
        return true;
    }

    public Map<String, Object> convertRecord(GenericRecord record) {
        Map<String, Object> map = new HashMap<>();
        Schema schema = record.getSchema();
        for (Field field : schema.getFields()) {
            map.put(field.name(), convertAvro(record.get(field.pos()), field.schema()));
        }
        return map;
    }

    private Object convertAvro(Object data, Schema schema) {
        switch (schema.getType()) {
            case RECORD:
                return convertRecord((GenericRecord) data);
            case MAP: {
                Map<String, Object> value = new HashMap<>();
                Schema valueType = schema.getValueType();
                for (Map.Entry<?, ?> entry : ((Map<?, ?>)data).entrySet()) {
                    value.put(entry.getKey().toString(), convertAvro(entry.getValue(), valueType));
                }
                return value;
            }
            case ARRAY: {
                List<?> origList = (List<?>)data;
                Schema itemType = schema.getElementType();
                List<Object> list = new ArrayList<>(origList.size());
                for (Object orig : origList) {
                    list.add(convertAvro(orig, itemType));
                }
                return list;
            }
            case UNION: {
                int type = new GenericData().resolveUnion(schema, data);
                return convertAvro(data, schema.getTypes().get(type));
            }
            case BYTES:
                return ((ByteBuffer)data).array();
            case FIXED:
                return ((GenericFixed)data).bytes();
            case ENUM:
            case STRING:
                return data.toString();
            case INT:
            case LONG:
            case DOUBLE:
            case FLOAT:
            case BOOLEAN:
            case NULL:
                return data;
            default:
                throw new IllegalArgumentException("Cannot parse field type " + schema.getType());
        }
    }

    @Override
    public void flush() throws IOException {
        generator.flush();
    }

    @Override
    public void close() throws IOException {
        generator.close();
    }
}
