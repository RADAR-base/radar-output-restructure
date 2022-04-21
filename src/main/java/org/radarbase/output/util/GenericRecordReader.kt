package org.radarbase.output.util

import org.apache.avro.file.DataFileReader
import org.apache.avro.file.SeekableInput
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericRecord
import org.radarbase.output.util.Timer.time
import java.io.Closeable

class GenericRecordReader(
    private val reader: DataFileReader<GenericRecord>
): Closeable, Iterator<GenericRecord> {
    private var tmpRecord: GenericRecord? = null

    constructor(input: SeekableInput) : this(DataFileReader(input,
        GenericDatumReader(null, null, GenericData().apply {
            isFastReaderEnabled = true
        })))

    override fun hasNext(): Boolean = reader.hasNext()

    override fun next(): GenericRecord {
        return time("read") {
            if (!hasNext()) {
                throw NoSuchElementException()
            }
            reader.next(tmpRecord)
        }.also { tmpRecord = it }
    }

    override fun close() {
        reader.close()
    }
}
