package org.radarcns.util;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;

/**
 * Created by joris on 20/04/2017.
 */
public class AvroConverter {
    public static Map<String, Object> convertRecord(GenericRecord record) {
        Map<String, Object> map = new HashMap<>();
        Schema schema = record.getSchema();
        for (Field field : schema.getFields()) {
            map.put(field.name(), convertAvro(record.get(field.pos()), field.schema()));
        }
        return map;
    }

    private static Object convertAvro(Object data, Schema schema) {
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
}
