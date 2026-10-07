package org.radarbase.output.format

import org.apache.avro.generic.GenericRecord
import org.radarbase.output.compression.Compression
import java.io.IOException
import java.io.InputStream
import java.io.Reader
import java.io.Writer
import java.nio.file.Path

/**
 * [RecordConverterFactory] for CDISC ODM v1.3 XML output.
 *
 * Field paths are configured via `format.properties` in `restructure.yml`.
 */
class OdmAvroConverterFactory : RecordConverterFactory {

    override val extension: String = ".xml"
    override val formats: Collection<String> = setOf("odm")

    var metadataVersionOid: String = OdmFieldConfig.DEFAULT_METADATA_VERSION_OID
        internal set

    var fieldConfig: OdmFieldConfig = OdmFieldConfig()
        internal set

    @Throws(IOException::class)
    override fun converterFor(
        writer: Writer,
        record: GenericRecord,
        writeHeader: Boolean,
        reader: Reader,
        excludeFields: Set<String>,
    ): RecordConverter {
        val studyOid = OdmAvroDataConverter.getStringAt(record, fieldConfig.projectIdField)
            ?: throw OdmConversionException(
                "Avro record is missing required field '${fieldConfig.projectIdField.joinToString(".")}'",
            )
        return OdmAvroConverter(writer, studyOid, metadataVersionOid, fieldConfig)
    }

    override suspend fun readTimeSeconds(
        source: InputStream,
        compression: Compression,
    ): Pair<Array<String>?, List<Double>>? = null

    override suspend fun contains(
        source: Path,
        record: GenericRecord,
        compression: Compression,
        usingFields: Set<String>,
        ignoreFields: Set<String>,
    ): Boolean = false

    override suspend fun deduplicate(
        fileName: String,
        source: Path,
        target: Path,
        compression: Compression,
        distinctFields: Set<String>,
        ignoreFields: Set<String>,
    ): Boolean = false
}
