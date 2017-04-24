package org.radarcns;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.csv.CsvFactory;
import com.fasterxml.jackson.dataformat.csv.CsvGenerator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.Flushable;
import java.io.IOException;

public class OffsetRangeFile implements Flushable, Closeable {
    private final CsvMapper mapper;
    private final CsvSchema schema;
    private final File file;
    private final FileWriter fileWriter;
    private final BufferedWriter bufferedWriter;
    private final CsvGenerator generator;
    private final ObjectWriter writer;

    public OffsetRangeFile(File file) throws IOException {
        this.file = file;
        boolean fileIsNew = !file.exists() || file.length() == 0;
        CsvFactory factory = new CsvFactory();
        this.mapper = new CsvMapper(factory);
        this.schema = mapper.schemaFor(OffsetRange.class);
        this.fileWriter = new FileWriter(file, true);
        this.bufferedWriter = new BufferedWriter(this.fileWriter);
        this.generator = factory.createGenerator(bufferedWriter);
        this.writer = mapper.writerFor(OffsetRange.class)
                .with(fileIsNew ? schema.withHeader() : schema);
    }

    public void write(OffsetRange range) throws IOException {
        writer.writeValue(generator, range);
    }

    public OffsetRangeSet read() throws IOException {
        OffsetRangeSet set = new OffsetRangeSet();
        ObjectReader reader = mapper.readerFor(OffsetRange.class).with(schema.withHeader());

        try (FileReader fr = new FileReader(file);
                BufferedReader br = new BufferedReader(fr)) {
            MappingIterator<OffsetRange> ranges = reader.readValues(br);
            while(ranges.hasNext()) {
                set.add(ranges.next());
            }
        }
        return set;
    }

    @Override
    public void flush() throws IOException {
        generator.flush();
    }

    @Override
    public void close() throws IOException {
        generator.close();
        bufferedWriter.close();
        fileWriter.close();
    }
}
