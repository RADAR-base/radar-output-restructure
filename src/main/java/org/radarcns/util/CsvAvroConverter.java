package org.radarcns.util;

import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.csv.CsvFactory;
import com.fasterxml.jackson.dataformat.csv.CsvGenerator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import java.io.IOException;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;

/**
 * Converts deep hierarchical Avro records into flat CSV format. It uses a simple dot syntax in the
 * column names to indicate hierarchy. After the first data record is added, all following
 * records need to have exactly the same hierarchy (or at least a subset of it.)
 */
public class CsvAvroConverter implements RecordConverter {

    public static RecordConverterFactory getFactory() {
        CsvFactory factory = new CsvFactory();
        return (writer, record, writeHeader) -> new CsvAvroConverter(factory, writer, record, writeHeader);
    }

    private final ObjectWriter csvWriter;
    private final Map<String, Object> map;
    private final CsvGenerator generator;

    public CsvAvroConverter(CsvFactory factory, Writer writer, GenericRecord record, boolean writeHeader)
            throws IOException {
        map = new LinkedHashMap<>();
        Map<String, Object> value = convertRecord(record);
        CsvSchema.Builder builder = new CsvSchema.Builder();
        for (String key : value.keySet()) {
            builder.addColumn(key);
        }
        CsvSchema schema = builder.build();
        if (writeHeader) {
            schema = schema.withHeader();
        }
        generator = factory.createGenerator(writer);
        csvWriter = new CsvMapper(factory).writer(schema);
    }

    @Override
    public void writeRecord(GenericRecord record) throws IOException {
        csvWriter.writeValue(generator, convertRecord(record));
    }

    public Map<String, Object> convertRecord(GenericRecord record) {
        map.clear();
        Schema schema = record.getSchema();
        for (Field field : schema.getFields()) {
            convertAvro(record.get(field.pos()), field.schema(), field.name());
        }
        return map;
    }

    private void convertAvro(Object data, Schema schema, String prefix) {
        switch (schema.getType()) {
            case RECORD: {
                GenericRecord record = (GenericRecord) data;
                Schema subSchema = record.getSchema();
                for (Field field : subSchema.getFields()) {
                    Object subData = record.get(field.pos());
                    convertAvro(subData, field.schema(), prefix + '.' + field.name());
                }
                break;
            }
            case MAP: {
                Schema valueType = schema.getValueType();
                for (Map.Entry<?, ?> entry : ((Map<?, ?>)data).entrySet()) {
                    String name = prefix + '.' + entry.getKey();
                    convertAvro(entry.getValue(), valueType, name);
                }
                break;
            }
            case ARRAY: {
                List<?> origList = (List<?>)data;
                Schema itemType = schema.getElementType();
                int i = 0;
                for (Object orig : origList) {
                    convertAvro(orig, itemType, prefix + '.' + i);
                    i++;
                }
                break;
            }
            case UNION: {
                int type = new GenericData().resolveUnion(schema, data);
                convertAvro(data, schema.getTypes().get(type), prefix);
                break;
            }
            case BYTES:
                map.put(prefix, ((ByteBuffer)data).array());
                break;
            case FIXED:
                map.put(prefix, ((GenericFixed)data).bytes());
                break;
            case ENUM:
            case STRING:
                map.put(prefix, data.toString());
                break;
            case INT:
            case LONG:
            case DOUBLE:
            case FLOAT:
            case BOOLEAN:
            case NULL:
                map.put(prefix, data);
                break;
            default:
                throw new IllegalArgumentException("Cannot parse field type " + schema.getType());
        }
    }


    @Override
    public void close() throws IOException {
        generator.close();
    }

    @Override
    public void flush() throws IOException {
        generator.flush();
    }
}
