package org.radarbase.output.format

import org.apache.avro.generic.GenericRecord
import java.io.IOException
import java.io.Writer
import javax.xml.stream.XMLOutputFactory
import javax.xml.stream.XMLStreamException
import javax.xml.stream.XMLStreamWriter

/**
 * Writes Avro [GenericRecord] values as a CDISC ODM v1.3 XML document.
 *
 * Field mappings and value transforms are configured via [OdmFieldConfig] (`format.properties`).
 * XML attribute values are escaped by the StAX writer.
 */
class OdmAvroConverter(
    private val writer: Writer,
    private val studyOid: String,
    private val metadataVersionOid: String,
    fieldConfig: OdmFieldConfig = OdmFieldConfig(),
) : RecordConverter {

    private val converter = OdmAvroDataConverter(fieldConfig)
    private val xmlWriter: XMLStreamWriter = XML_FACTORY.createXMLStreamWriter(writer)

    init {
        xmlWriter.writeStartDocument("UTF-8", "1.0")
        xmlWriter.writeCharacters("\n")
        xmlWriter.writeStartElement(ELEM_ODM)
        xmlWriter.writeDefaultNamespace(ODM_NAMESPACE)
        xmlWriter.writeAttribute(ATTR_FILE_TYPE, FILE_TYPE_TRANSACTIONAL)
        xmlWriter.writeCharacters("\n    ")
        xmlWriter.writeStartElement(ELEM_CLINICAL_DATA)
        xmlWriter.writeAttribute(ATTR_STUDY_OID, studyOid)
        xmlWriter.writeAttribute(ATTR_METADATA_VERSION_OID, metadataVersionOid)
    }

    /**
     * Converts [record] and appends a `<SubjectData>` element.
     *
     * Returns `false` (without writing) when conversion fails, signalling that this record
     * cannot be placed in the current output file.
     */
    @Throws(IOException::class)
    override fun writeRecord(record: GenericRecord): Boolean {
        return try {
            writeOdmRecord(converter.toOdmRecord(record))
            true
        } catch (_: OdmConversionException) {
            false
        }
    }

    override fun convertRecord(record: GenericRecord): Map<String, Any?> =
        throw UnsupportedOperationException("ODM converter does not support flat record conversion")

    @Throws(IOException::class)
    override fun flush() {
        xmlWriter.flush()
        writer.flush()
    }

    @Throws(IOException::class)
    override fun close() {
        try {
            xmlWriter.writeCharacters("\n    ")
            xmlWriter.writeEndElement() // </ClinicalData>
            xmlWriter.writeCharacters("\n")
            xmlWriter.writeEndElement() // </ODM>
            xmlWriter.writeEndDocument()
            xmlWriter.flush()
        } catch (e: XMLStreamException) {
            throw IOException("Failed to close ODM XML document", e)
        } finally {
            xmlWriter.close()
            writer.close()
        }
    }

    private fun writeOdmRecord(odmRecord: OdmRecord) {
        xmlWriter.writeCharacters("\n        ")
        xmlWriter.writeStartElement(ELEM_SUBJECT_DATA)
        xmlWriter.writeAttribute(ATTR_SUBJECT_KEY, odmRecord.subjectKey)

        xmlWriter.writeCharacters("\n            ")
        xmlWriter.writeStartElement(ELEM_STUDY_EVENT_DATA)
        xmlWriter.writeAttribute(ATTR_STUDY_EVENT_OID, odmRecord.studyEventOid)
        odmRecord.studyEventRepeatKey?.let {
            xmlWriter.writeAttribute(ATTR_STUDY_EVENT_REPEAT_KEY, it)
        }

        xmlWriter.writeCharacters("\n                ")
        xmlWriter.writeStartElement(ELEM_FORM_DATA)
        xmlWriter.writeAttribute(ATTR_FORM_OID, odmRecord.formOid)

        xmlWriter.writeCharacters("\n                    ")
        xmlWriter.writeStartElement(ELEM_ITEM_GROUP_DATA)
        xmlWriter.writeAttribute(ATTR_ITEM_GROUP_OID, odmRecord.itemGroupOid)
        xmlWriter.writeAttribute(ATTR_IG_REPEAT_KEY, odmRecord.igRepeatKey)

        for (item in odmRecord.items) {
            xmlWriter.writeCharacters("\n                        ")
            xmlWriter.writeEmptyElement(ELEM_ITEM_DATA)
            xmlWriter.writeAttribute(ATTR_ITEM_OID, item.itemOid)
            xmlWriter.writeAttribute(ATTR_VALUE, item.value)
        }

        xmlWriter.writeCharacters("\n                    ")
        xmlWriter.writeEndElement() // </ItemGroupData>
        xmlWriter.writeCharacters("\n                ")
        xmlWriter.writeEndElement() // </FormData>
        xmlWriter.writeCharacters("\n            ")
        xmlWriter.writeEndElement() // </StudyEventData>
        xmlWriter.writeCharacters("\n        ")
        xmlWriter.writeEndElement() // </SubjectData>
    }

    companion object {
        const val ODM_NAMESPACE = "http://www.cdisc.org/ns/odm/v1.3"
        private const val FILE_TYPE_TRANSACTIONAL = "Transactional"

        private const val ELEM_ODM = "ODM"
        private const val ELEM_CLINICAL_DATA = "ClinicalData"
        private const val ELEM_SUBJECT_DATA = "SubjectData"
        private const val ELEM_STUDY_EVENT_DATA = "StudyEventData"
        private const val ELEM_FORM_DATA = "FormData"
        private const val ELEM_ITEM_GROUP_DATA = "ItemGroupData"
        private const val ELEM_ITEM_DATA = "ItemData"

        private const val ATTR_FILE_TYPE = "FileType"
        private const val ATTR_STUDY_OID = "StudyOID"
        private const val ATTR_METADATA_VERSION_OID = "MetaDataVersionOID"
        private const val ATTR_SUBJECT_KEY = "SubjectKey"
        private const val ATTR_STUDY_EVENT_OID = "StudyEventOID"
        private const val ATTR_STUDY_EVENT_REPEAT_KEY = "StudyEventRepeatKey"
        private const val ATTR_FORM_OID = "FormOID"
        private const val ATTR_ITEM_GROUP_OID = "ItemGroupOID"
        private const val ATTR_IG_REPEAT_KEY = "IGRepeatKey"
        private const val ATTR_ITEM_OID = "ItemOID"
        private const val ATTR_VALUE = "Value"

        private val XML_FACTORY: XMLOutputFactory = XMLOutputFactory.newInstance()

        val factory: RecordConverterFactory = OdmAvroConverterFactory()
    }
}
