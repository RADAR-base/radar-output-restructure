package org.radarcns.util;

import java.io.IOException;
import java.io.Writer;
import org.apache.avro.generic.GenericRecord;

public interface RecordConverterFactory {
    RecordConverter converterFor(Writer writer, GenericRecord record, boolean writeHeader) throws IOException;
}
