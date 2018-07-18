package org.radarcns.hdfs.data;

import java.util.Arrays;
import java.util.List;

public class FormatFactory implements FormatProvider<RecordConverterFactory> {
    @Override
    public List<RecordConverterFactory> getAll() {
        return Arrays.asList(
                CsvAvroConverter.getFactory(),
                JsonAvroConverter.getFactory());
    }
}
