package org.radarbase.output.data

import org.apache.avro.Schema.Parser
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.DecoderFactory
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.radarbase.output.format.OdmAvroConverterFactory
import org.radarbase.output.format.OdmAvroDataConverter
import org.radarbase.output.format.OdmConversionException
import org.radarbase.output.format.OdmFieldConfig
import java.io.IOException
import java.io.StringReader
import java.io.StringWriter

class OdmAvroConverterTest {
    @Test
    @Throws(IOException::class)
    fun writeQuestionnaireRecord() {
        val record = readQuestionnaireRecord()
        val writer = StringWriter()
        val factory = OdmAvroConverterFactory()
        val converter = factory.converterFor(writer, record, false, StringReader(""), emptySet())

        assertTrue(converter.writeRecord(record))
        converter.close()

        val xml = writer.toString()
        assertTrue(xml.contains("StudyOID=\"RECURRENT-GB\""))
        assertTrue(xml.contains("SubjectKey=\"5b3adcb0\""))
        assertTrue(xml.contains("StudyEventOID=\"Weekly|W6\""))
        assertTrue(xml.contains("StudyEventRepeatKey=\"W6\""))
        assertTrue(xml.contains("FormOID=\"Weekly\""))
        assertTrue(xml.contains("ItemGroupOID=\"Weekly_IG\""))
        assertTrue(xml.contains("IGRepeatKey=\"1\""))
        assertTrue(xml.contains("ItemOID=\"wqpa_date\""))
        assertTrue(xml.contains("Value=\"2026-03-20\""))
        assertTrue(xml.contains("ItemOID=\"wqpa_q29\""))
        assertTrue(xml.contains("Value=\"5\""))
    }

    @Test
    @Throws(IOException::class)
    fun writeQuestionnaireRecordWithCustomFieldPaths() {
        val record = readQuestionnaireRecord()
        val writer = StringWriter()
        val factory = OdmAvroConverterFactory().apply {
            fieldConfig = OdmFieldConfig(subjectKeyField = listOf("key", "sourceId"))
        }
        val converter = factory.converterFor(writer, record, false, StringReader(""), emptySet())

        assertTrue(converter.writeRecord(record))
        converter.close()

        val xml = writer.toString()
        assertTrue(xml.contains("SubjectKey=\"0b5da503\""))
    }

    @Test
    fun fieldConfigFromProperties() {
        val config = OdmFieldConfig.fromProperties(
            mapOf(
                "subjectKeyField" to "key.sourceId",
                "studyEventOidField" to "value.name",
                "answersField" to "value.answers",
                "itemOidField" to "questionId",
                "itemValueField" to "value",
                "itemGroupOidTemplate" to "{formOid}-group",
                "igRepeatKey" to "2",
                "repeatKeySeparator" to "",
                "dateInputFormat" to """{"day":dd,"month":MMM,"year":yyyy}""",
                "dateOutputFormat" to "yyyy-MM-dd",
            ),
        )
        assertEquals(listOf("key", "sourceId"), config.subjectKeyField)
        assertEquals("{formOid}-group", config.itemGroupOidTemplate)
        assertEquals("2", config.igRepeatKey)
        assertEquals("", config.repeatKeySeparator)
        assertEquals("yyyy-MM-dd", config.dateOutputFormat)
    }

    @Test
    @Throws(IOException::class)
    fun writeQuestionnaireRecordWithoutRepeatKeySeparator() {
        val record = readQuestionnaireRecord()
        val factory = OdmAvroConverterFactory().apply {
            fieldConfig = OdmFieldConfig(repeatKeySeparator = "")
        }
        val writer = StringWriter()
        val converter = factory.converterFor(writer, record, false, StringReader(""), emptySet())
        assertTrue(converter.writeRecord(record))
        converter.close()
        val xml = writer.toString()
        assertTrue(xml.contains("StudyEventOID=\"Weekly|W6\""))
        assertFalse(xml.contains("StudyEventRepeatKey"))
        assertTrue(xml.contains("FormOID=\"Weekly|W6\""))
    }

    @Test
    @Throws(IOException::class)
    fun xmlAttributeValuesAreEscapedOnWrite() {
        // OdmAvroConverter delegates attribute escaping to StAX XMLStreamWriter.writeAttribute
        val writer = StringWriter()
        val xmlWriter = javax.xml.stream.XMLOutputFactory.newInstance().createXMLStreamWriter(writer)
        xmlWriter.writeEmptyElement("ItemData")
        xmlWriter.writeAttribute("Value", "a&b<c>\"")
        xmlWriter.close()
        val xml = writer.toString()
        assertTrue(xml.contains("&amp;"))
        assertTrue(xml.contains("&lt;"))
        assertFalse(xml.contains("Value=\"a&b"))
    }

    @Test
    fun numericMonthJsonTemplate() {
        val record = readQuestionnaireRecord()
        val answers = (record.get("value") as GenericRecord).get("answers") as List<*>
        (answers.first() as GenericRecord).put("value", """{"day":"20","month":"3","year":"2026"}""")

        val odmRecord = OdmAvroDataConverter(
            OdmFieldConfig(
                dateInputFormat = """{"day":dd,"month":M,"year":yyyy}""",
                dateOutputFormat = "yyyy-MM-dd",
            ),
        ).toOdmRecord(record)
        assertEquals("2026-03-20", odmRecord.items.find { it.itemOid == "wqpa_date" }?.value)
    }

    @Test
    fun customDateOutputFormat() {
        val record = readQuestionnaireRecord()
        val odmRecord = OdmAvroDataConverter(
            OdmFieldConfig(
                dateInputFormat = """{"day":dd,"month":MMM,"year":yyyy}""",
                dateOutputFormat = "yyyy-MM-dd",
            ),
        ).toOdmRecord(record)
        assertEquals("2026-03-20", odmRecord.items.find { it.itemOid == "wqpa_date" }?.value)
    }

    @Test
    fun plainStringDateInputFormat() {
        val record = readQuestionnaireRecord()
        val answers = (record.get("value") as GenericRecord).get("answers") as List<*>
        (answers.first() as GenericRecord).put("value", "20/03/2026")

        val odmRecord = OdmAvroDataConverter(
            OdmFieldConfig(dateInputFormat = "dd/MM/yyyy", dateOutputFormat = "yyyy-MM-dd"),
        ).toOdmRecord(record)
        assertEquals("2026-03-20", odmRecord.items.find { it.itemOid == "wqpa_date" }?.value)
    }

    @Test
    fun disabledDateNormalisation() {
        val record = readQuestionnaireRecord()
        val odmRecord = OdmAvroDataConverter(
            OdmFieldConfig(dateInputFormat = "", dateOutputFormat = ""),
        ).toOdmRecord(record)
        assertTrue(odmRecord.items.find { it.itemOid == "wqpa_date" }!!.value.startsWith("{"))
    }

    @Test
    fun missingAnswersFieldFailsLoud() {
        val record = readQuestionnaireRecord()
        assertThrows(OdmConversionException::class.java) {
            OdmAvroDataConverter(OdmFieldConfig(answersField = listOf("value", "missing"))).toOdmRecord(record)
        }
    }

    @Test
    fun writeRecordReturnsFalseWhenRequiredFieldMissing() {
        val record = readQuestionnaireRecord()
        val factory = OdmAvroConverterFactory().apply {
            fieldConfig = OdmFieldConfig(subjectKeyField = listOf("key", "missing"))
        }
        val converter = factory.converterFor(StringWriter(), record, false, StringReader(""), emptySet())
        assertFalse(converter.writeRecord(record))
    }

    private fun readQuestionnaireRecord(): GenericRecord {
        val schema = Parser().parse(javaClass.getResourceAsStream("questionnaire.avsc"))
        val reader = GenericDatumReader<GenericRecord>(schema)
        val decoder = DecoderFactory.get().jsonDecoder(schema, javaClass.getResourceAsStream("questionnaire.json"))
        return reader.read(null, decoder)
    }
}
