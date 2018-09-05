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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
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
    private static final Pattern ESCAPE_PATTERN = Pattern.compile("\\p{C}|[,\"]");
    private static final Pattern TAB_PATTERN = Pattern.compile("\t");
    private static final Pattern LINE_ENDING_PATTERN = Pattern.compile("[\n\r]+");
    private static final Pattern NON_PRINTING_PATTERN = Pattern.compile("\\p{C}");
    private static final Pattern QUOTE_OR_COMMA_PATTERN = Pattern.compile("[\",]");
    private static final Pattern QUOTE_PATTERN = Pattern.compile("\"");
    private static final Base64.Encoder BASE64_ENCODER = Base64.getEncoder().withoutPadding();
    private final List<String> headers;
    private final List<String> values;
    private final Writer writer;

    public static RecordConverterFactory getFactory() {
        return new RecordConverterFactory() {
            @Override
            public CsvAvroConverter converterFor(Writer writer, GenericRecord record,
                    boolean writeHeader, Reader reader) throws IOException {
                return new CsvAvroConverter(writer, record,
                        writeHeader, reader);
            }

            @Override
            public boolean hasHeader() {
                return true;
            }

            @Override
            public String getExtension() {
                return ".csv";
            }

            @Override
            public Collection<String> getFormats() {
                return Collections.singleton("csv");
            }
        };
    }

    public CsvAvroConverter(Writer writer, GenericRecord record, boolean writeHeader,
            Reader reader) throws IOException {
        this.writer = writer;

        if (writeHeader) {
            headers = createHeaders(record);
            writeLine(headers);
        } else {
            // If file already exists read the schema from the CSV file
            try (BufferedReader bufReader = new BufferedReader(reader, 200)) {
                headers = parseCsvLine(bufReader);
            }
        }

        values = new ArrayList<>(headers.size());
    }

    static List<String> parseCsvLine(Reader reader) throws IOException {
        List<String> headers = new ArrayList<>();
        StringBuilder builder = new StringBuilder(20);
        int ch = reader.read();
        boolean inString = false;
        boolean hadQuote = false;

        while (ch != '\n' && ch != '\r' && ch != -1) {
            switch (ch) {
                case '"':
                    if (inString) {
                        inString = false;
                        hadQuote = true;
                    } else if (hadQuote) {
                        inString = true;
                        hadQuote = false;
                        builder.append('"');
                    } else {
                        inString = true;
                    }
                    break;
                case ',':
                    if (inString) {
                        builder.append(',');
                    } else {
                        if (hadQuote) {
                            hadQuote = false;
                        }
                        headers.add(builder.toString());
                        builder = new StringBuilder(20);
                    }
                    break;
                default:
                    builder.append((char)ch);
                    break;
            }
            ch = reader.read();
        }
        headers.add(builder.toString());
        return headers;
    }

    /**
     * Write AVRO record to CSV file.
     * @param record the AVRO record to be written to CSV file
     * @return true if write was successful, false if cannot write record to the current CSV file
     * @throws IOException for other IO and Mapping errors
     */
    @Override
    public boolean writeRecord(GenericRecord record) throws IOException {
        try {
            List<String> retValues = convertRecordValues(record);
            if (retValues.size() < headers.size()) {
                return false;
            }

            writeLine(retValues);
            return true;
        } catch (IllegalArgumentException | IndexOutOfBoundsException ex) {
            return false;
        }
    }

    private void writeLine(List<?> objects) throws IOException {
        for (int i = 0; i < objects.size(); i++) {
            if (i > 0) {
                writer.append(',');
            }
            writer.append(objects.get(i).toString());
        }
        writer.append('\n');
    }


    public Map<String, Object> convertRecord(GenericRecord record) {
        values.clear();
        Schema schema = record.getSchema();
        for (Field field : schema.getFields()) {
            convertAvro(values, record.get(field.pos()), field.schema(), field.name());
        }
        Map<String, Object> map = new LinkedHashMap<>();
        for (int i = 0; i < headers.size(); i++) {
            map.put(headers.get(i), values.get(i));
        }
        values.clear();
        return map;
    }

    public List<String> convertRecordValues(GenericRecord record) {
        values.clear();
        Schema schema = record.getSchema();
        for (Field field : schema.getFields()) {
            convertAvro(values, record.get(field.pos()), field.schema(), field.name());
        }
        return values;
    }

    static List<String> createHeaders(GenericRecord record) {
        List<String> headers = new ArrayList<>();
        Schema schema = record.getSchema();
        for (Field field : schema.getFields()) {
            createHeader(headers, record.get(field.pos()), field.schema(), field.name());
        }
        return headers;
    }

    private void convertAvro(List<String> values, Object data, Schema schema, String prefix) {
        switch (schema.getType()) {
            case RECORD: {
                GenericRecord record = (GenericRecord) data;
                Schema subSchema = record.getSchema();
                for (Field field : subSchema.getFields()) {
                    Object subData = record.get(field.pos());
                    convertAvro(values, subData, field.schema(), prefix + '.' + field.name());
                }
                break;
            }
            case MAP: {
                Schema valueType = schema.getValueType();
                for (Map.Entry<?, ?> entry : ((Map<?, ?>)data).entrySet()) {
                    String name = prefix + '.' + entry.getKey();
                    convertAvro(values, entry.getValue(), valueType, name);
                }
                break;
            }
            case ARRAY: {
                Schema itemType = schema.getElementType();
                int i = 0;
                for (Object orig : (List<?>)data) {
                    convertAvro(values, orig, itemType, prefix + '.' + i);
                    i++;
                }
                break;
            }
            case UNION: {
                int type = new GenericData().resolveUnion(schema, data);
                convertAvro(values, data, schema.getTypes().get(type), prefix);
                break;
            }
            case BYTES:
                checkHeader(prefix, values.size());
                values.add(BASE64_ENCODER.encodeToString(((ByteBuffer) data).array()));
                break;
            case FIXED:
                checkHeader(prefix, values.size());
                values.add(BASE64_ENCODER.encodeToString(((GenericFixed)data).bytes()));
                break;
            case STRING:
                checkHeader(prefix, values.size());
                values.add(cleanCsvString(data.toString()));
                break;
            case ENUM:
            case INT:
            case LONG:
            case DOUBLE:
            case FLOAT:
            case BOOLEAN:
                checkHeader(prefix, values.size());
                values.add(data.toString());
                break;
            case NULL:
                checkHeader(prefix, values.size());
                values.add("");
                break;
            default:
                throw new IllegalArgumentException("Cannot parse field type " + schema.getType());
        }
    }

    private void checkHeader(String prefix, int size) {
        if (!prefix.equals(headers.get(size))) {
            throw new IllegalArgumentException("Header " + prefix + " does not match "
                    + headers.get(size));
        }
    }

    static String cleanCsvString(String orig) {
        if (ESCAPE_PATTERN.matcher(orig).find()) {
            String cleaned = LINE_ENDING_PATTERN.matcher(orig).replaceAll("\\\\n");
            cleaned = TAB_PATTERN.matcher(cleaned).replaceAll("    ");
            cleaned = NON_PRINTING_PATTERN.matcher(cleaned).replaceAll("?");
            if (QUOTE_OR_COMMA_PATTERN.matcher(cleaned).find()) {
                return '"' + QUOTE_PATTERN.matcher(cleaned).replaceAll("\"\"") + '"';
            } else {
                return cleaned;
            }
        } else {
            return orig;
        }
    }

    private static void createHeader(List<String> headers, Object data, Schema schema, String prefix) {
        switch (schema.getType()) {
            case RECORD: {
                GenericRecord record = (GenericRecord) data;
                Schema subSchema = record.getSchema();
                for (Field field : subSchema.getFields()) {
                    Object subData = record.get(field.pos());
                    createHeader(headers, subData, field.schema(), prefix + '.' + field.name());
                }
                break;
            }
            case MAP: {
                Schema valueType = schema.getValueType();
                for (Map.Entry<?, ?> entry : ((Map<?, ?>)data).entrySet()) {
                    String name = prefix + '.' + entry.getKey();
                    createHeader(headers, entry.getValue(), valueType, name);
                }
                break;
            }
            case ARRAY: {
                Schema itemType = schema.getElementType();
                int i = 0;
                for (Object orig : (List<?>)data) {
                    createHeader(headers, orig, itemType, prefix + '.' + i);
                    i++;
                }
                break;
            }
            case UNION: {
                int type = new GenericData().resolveUnion(schema, data);
                createHeader(headers, data, schema.getTypes().get(type), prefix);
                break;
            }
            case BYTES:
            case FIXED:
            case ENUM:
            case STRING:
            case INT:
            case LONG:
            case DOUBLE:
            case FLOAT:
            case BOOLEAN:
            case NULL:
                headers.add(prefix);
                break;
            default:
                throw new IllegalArgumentException("Cannot parse field type " + schema.getType());
        }
    }

    @Override
    public void close() throws IOException {
        writer.close();
    }

    @Override
    public void flush() throws IOException {
        writer.flush();
    }
}
