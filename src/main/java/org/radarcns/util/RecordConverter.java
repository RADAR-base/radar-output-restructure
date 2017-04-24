package org.radarcns.util;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;

public interface RecordConverter extends Flushable, Closeable {
    void writeRecord(GenericRecord record) throws IOException;
    Map<String, Object> convertRecord(GenericRecord record);
}
