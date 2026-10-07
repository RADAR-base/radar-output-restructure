package org.radarbase.output.format

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import java.time.LocalDate
import java.time.format.DateTimeFormatter

/**
 * Extracts an [OdmRecord] from an Avro [GenericRecord] containing clinical questionnaire data.
 *
 * All Avro field paths and value transforms are configurable via [OdmFieldConfig] (from
 * `restructure.yml` `format.properties`).
 */
internal class OdmAvroDataConverter(
    private val fieldConfig: OdmFieldConfig = OdmFieldConfig(),
) {

    fun toOdmRecord(record: GenericRecord): OdmRecord {
        val subjectKey = requireString(record, fieldConfig.subjectKeyField)
        val studyEventOid = requireString(record, fieldConfig.studyEventOidField)

        val rawFormSource = if (fieldConfig.formOidField == fieldConfig.studyEventOidField) {
            studyEventOid
        } else {
            requireString(record, fieldConfig.formOidField)
        }

        val separator = fieldConfig.repeatKeySeparator
        val pipeIdx = if (separator.isEmpty()) -1 else rawFormSource.indexOf(separator)
        val formOid = if (pipeIdx >= 0) rawFormSource.substring(0, pipeIdx) else rawFormSource
        val repeatKey = if (pipeIdx >= 0) {
            rawFormSource.substring(pipeIdx + separator.length).takeIf { it.isNotBlank() }
        } else {
            null
        }
        val itemGroupOid = fieldConfig.itemGroupOidTemplate.replace(FORM_OID_PLACEHOLDER, formOid)

        return OdmRecord(
            subjectKey = subjectKey,
            studyEventOid = studyEventOid,
            studyEventRepeatKey = repeatKey,
            formOid = formOid,
            itemGroupOid = itemGroupOid,
            igRepeatKey = fieldConfig.igRepeatKey,
            items = extractAnswers(record),
        )
    }

    private fun requireString(record: GenericRecord, fieldPath: List<String>): String {
        val path = fieldPath.joinToString(".")
        return getStringAt(record, fieldPath)
            ?: throw OdmConversionException("Avro record is missing required field '$path'")
    }

    private fun extractAnswers(record: GenericRecord): List<OdmItem> {
        val path = fieldConfig.answersField.joinToString(".")
        val answers = getListAt(record, fieldConfig.answersField)
            ?: throw OdmConversionException("Avro record is missing required answers field '$path'")
        if (answers.isEmpty()) {
            throw OdmConversionException("Avro record answers field '$path' is empty")
        }

        val items = answers.filterIsInstance<GenericRecord>().mapNotNull { answer ->
            val questionId = getStringAt(answer, fieldConfig.itemOidField)
                ?: return@mapNotNull null
            val rawValue = getValueAt(answer, fieldConfig.itemValueField)?.toString() ?: ""
            OdmItem(itemOid = questionId, value = transformValue(rawValue))
        }
        if (items.isEmpty()) {
            throw OdmConversionException(
                "Avro record answers field '$path' contains no valid items " +
                    "(check itemOidField='${fieldConfig.itemOidField.joinToString(".")}')",
            )
        }
        return items
    }

    private fun transformValue(rawValue: String): String {
        val inputFormat = fieldConfig.dateInputFormat
        val outputFormat = fieldConfig.dateOutputFormat
        if (inputFormat.isBlank() || outputFormat.isBlank()) {
            return rawValue
        }

        val localDate = parseInputDate(rawValue, inputFormat) ?: return rawValue

        return try {
            DateTimeFormatter.ofPattern(outputFormat).format(localDate)
        } catch (ex: IllegalArgumentException) {
            throw OdmConversionException("Invalid dateOutputFormat pattern '$outputFormat': ${ex.message}")
        }
    }

    /**
     * Parses a date from [rawValue] using [inputFormat]:
     *  - `{"day":dd,"month":MMM,"year":yyyy}` — JSON object; keys are field names, values are format tokens
     *  - any other pattern — plain string parsed with [DateTimeFormatter] (e.g. `dd/MM/yyyy`)
     */
    private fun parseInputDate(rawValue: String, inputFormat: String): LocalDate? {
        val trimmed = rawValue.trim()
        val format = inputFormat.trim()
        return if (isJsonDateTemplate(format)) {
            parseJsonDateWithTemplate(trimmed, parseJsonDateTemplate(format))
        } else {
            try {
                LocalDate.parse(trimmed, DateTimeFormatter.ofPattern(format))
            } catch (_: Exception) {
                null
            }
        }
    }

    private fun isJsonDateTemplate(format: String): Boolean {
        val trimmed = format.trim()
        return trimmed.startsWith("{") && trimmed.endsWith("}") && trimmed.contains(':')
    }

    /** Parses `{"day":dd,"month":MMM,"year":yyyy}` into a map of JSON field name → pattern token. */
    private fun parseJsonDateTemplate(template: String): Map<String, String> {
        val trimmed = template.trim()
        if (!isJsonDateTemplate(trimmed)) {
            throw OdmConversionException(
                "dateInputFormat JSON template must look like {\"day\":dd,\"month\":MMM,\"year\":yyyy}",
            )
        }
        val fields = trimmed.substring(1, trimmed.length - 1)
            .split(',')
            .mapNotNull { segment ->
                val colon = segment.indexOf(':')
                if (colon <= 0) return@mapNotNull null
                val key = segment.substring(0, colon).trim().trim('"', '\'')
                val pattern = segment.substring(colon + 1).trim().trim('"', '\'')
                if (key.isEmpty() || pattern.isEmpty()) null else key to pattern
            }
        if (fields.size < 3) {
            throw OdmConversionException(
                "dateInputFormat '$template' must define day, month, and year fields",
            )
        }
        val hasDay = fields.any { 'd' in it.second && 'y' !in it.second }
        val hasMonth = fields.any { 'M' in it.second }
        val hasYear = fields.any { 'y' in it.second }
        if (!hasDay || !hasMonth || !hasYear) {
            throw OdmConversionException(
                "dateInputFormat '$template' must include day (d), month (M), and year (y) pattern tokens",
            )
        }
        return fields.toMap()
    }

    private fun parseJsonDateWithTemplate(rawValue: String, fieldPatterns: Map<String, String>): LocalDate? {
        if (!rawValue.startsWith('{')) return null
        return try {
            val node = OBJECT_MAPPER.readTree(rawValue)
            var day: Int? = null
            var month: Int? = null
            var year: Int? = null
            for ((field, pattern) in fieldPatterns) {
                val text = node.get(field)?.asText() ?: return null
                when {
                    'y' in pattern -> year = text.toIntOrNull()
                    'M' in pattern -> month = parseMonthValue(text, pattern)
                    'd' in pattern -> day = text.toIntOrNull()
                }
            }
            if (day == null || month == null || year == null) return null
            LocalDate.of(year, month, day)
        } catch (_: Exception) {
            null
        }
    }

    private fun parseMonthValue(text: String, pattern: String): Int? = when {
        pattern.count { it == 'M' } >= 3 -> MONTH_ABBR[text.uppercase().take(3)]
        else -> text.toIntOrNull()
    }

    companion object {
        private const val FORM_OID_PLACEHOLDER = "{formOid}"
        private val GENERIC_DATA = GenericData()
        private val OBJECT_MAPPER = ObjectMapper()

        private val MONTH_ABBR = mapOf(
            "JAN" to 1, "FEB" to 2, "MAR" to 3, "APR" to 4,
            "MAY" to 5, "JUN" to 6, "JUL" to 7, "AUG" to 8,
            "SEP" to 9, "OCT" to 10, "NOV" to 11, "DEC" to 12,
        )

        internal fun getStringAt(record: GenericRecord, fieldPath: List<String>): String? =
            getValueAt(record, fieldPath)?.toString()?.takeIf { it.isNotBlank() }

        internal fun getListAt(record: GenericRecord, fieldPath: List<String>): List<*>? =
            getValueAt(record, fieldPath) as? List<*>

        internal fun getValueAt(record: GenericRecord, fieldPath: List<String>): Any? {
            var current: Any? = record
            for (name in fieldPath) {
                current = when (current) {
                    is GenericRecord -> resolveField(current, name)
                    is Map<*, *> -> current[name]
                        ?: current.entries.firstOrNull { it.key?.toString() == name }?.value
                    else -> return null
                } ?: return null
            }
            return current
        }

        internal fun resolveField(record: GenericRecord, fieldName: String): Any? {
            val field = record.schema.getField(fieldName) ?: return null
            return resolveUnion(record.get(field.pos()), field.schema())
        }

        private fun resolveUnion(data: Any?, schema: Schema): Any? {
            if (schema.type != Schema.Type.UNION) return data
            val index = GENERIC_DATA.resolveUnion(schema, data)
            return resolveUnion(data, schema.types[index])
        }
    }
}

/**
 * Configuration for which Avro field paths map to ODM structural fields.
 *
 * Paths use dot notation relative to the root combined key-value record (e.g. `key.userId`,
 * `value.answers`). Answer item fields ([itemOidField], [itemValueField]) are relative to each
 * element of the [answersField] array.
 *
 * Configure via `format.properties` in `restructure.yml`.
 */
data class OdmFieldConfig(
    val subjectKeyField: List<String> = listOf("key", "userId"),
    val projectIdField: List<String> = listOf("key", "projectId"),
    val studyEventOidField: List<String> = listOf("value", "name"),
    val formOidField: List<String> = listOf("value", "name"),
    val answersField: List<String> = listOf("value", "answers"),
    val itemOidField: List<String> = listOf("questionId"),
    val itemValueField: List<String> = listOf("value"),
    /** Template for ItemGroupOID; `{formOid}` is substituted. Default: `{formOid}_IG`. */
    val itemGroupOidTemplate: String = DEFAULT_ITEM_GROUP_OID_TEMPLATE,
    val igRepeatKey: String = DEFAULT_IG_REPEAT_KEY,
    val repeatKeySeparator: String = "|",
    /**
     * Input format for date-like answer values:
     *  - `{"day":dd,"month":MMM,"year":yyyy}` — JSON object template (default for questionnaire widgets)
     *  - Java date pattern — plain string (e.g. `dd/MM/yyyy`, `yyyy-MM-dd`)
     * Leave empty to disable date normalisation.
     */
    val dateInputFormat: String = DEFAULT_DATE_INPUT_FORMAT,
    /** Output [DateTimeFormatter] pattern for normalised dates (REDCap requires `yyyy-MM-dd`). Leave empty to disable. */
    val dateOutputFormat: String = DEFAULT_DATE_OUTPUT_FORMAT,
) {
    companion object {
        const val PROP_METADATA_VERSION_OID = "metadataVersionOid"
        const val PROP_SUBJECT_KEY_FIELD = "subjectKeyField"
        const val PROP_PROJECT_ID_FIELD = "projectIdField"
        const val PROP_STUDY_EVENT_OID_FIELD = "studyEventOidField"
        const val PROP_FORM_OID_FIELD = "formOidField"
        const val PROP_ANSWERS_FIELD = "answersField"
        const val PROP_ITEM_OID_FIELD = "itemOidField"
        const val PROP_ITEM_VALUE_FIELD = "itemValueField"
        const val PROP_ITEM_GROUP_OID_TEMPLATE = "itemGroupOidTemplate"
        const val PROP_IG_REPEAT_KEY = "igRepeatKey"
        const val PROP_REPEAT_KEY_SEPARATOR = "repeatKeySeparator"
        const val PROP_DATE_INPUT_FORMAT = "dateInputFormat"
        const val PROP_DATE_OUTPUT_FORMAT = "dateOutputFormat"

        const val DEFAULT_METADATA_VERSION_OID = "v1.0.0"
        const val DEFAULT_ITEM_GROUP_OID_TEMPLATE = "{formOid}_IG"
        const val DEFAULT_IG_REPEAT_KEY = "1"
        const val DEFAULT_DATE_INPUT_FORMAT = """{"day":dd,"month":MMM,"year":yyyy}"""
        const val DEFAULT_DATE_OUTPUT_FORMAT = "yyyy-MM-dd"

        fun fromDotPath(dotPath: String): List<String> = dotPath.split('.')

        private fun pathProperty(
            properties: Map<String, String>,
            key: String,
            default: List<String>,
        ): List<String> = properties[key]?.let { fromDotPath(it) } ?: default

        fun fromProperties(properties: Map<String, String>): OdmFieldConfig {
            val defaults = OdmFieldConfig()
            val studyEventOidField = pathProperty(properties, PROP_STUDY_EVENT_OID_FIELD, defaults.studyEventOidField)
            val itemGroupOidTemplate = properties[PROP_ITEM_GROUP_OID_TEMPLATE]
                ?: defaults.itemGroupOidTemplate
            return OdmFieldConfig(
                subjectKeyField = pathProperty(properties, PROP_SUBJECT_KEY_FIELD, defaults.subjectKeyField),
                projectIdField = pathProperty(properties, PROP_PROJECT_ID_FIELD, defaults.projectIdField),
                studyEventOidField = studyEventOidField,
                formOidField = pathProperty(properties, PROP_FORM_OID_FIELD, studyEventOidField),
                answersField = pathProperty(properties, PROP_ANSWERS_FIELD, defaults.answersField),
                itemOidField = pathProperty(properties, PROP_ITEM_OID_FIELD, defaults.itemOidField),
                itemValueField = pathProperty(properties, PROP_ITEM_VALUE_FIELD, defaults.itemValueField),
                itemGroupOidTemplate = itemGroupOidTemplate,
                igRepeatKey = properties[PROP_IG_REPEAT_KEY] ?: defaults.igRepeatKey,
                repeatKeySeparator = properties[PROP_REPEAT_KEY_SEPARATOR] ?: defaults.repeatKeySeparator,
                dateInputFormat = properties[PROP_DATE_INPUT_FORMAT] ?: defaults.dateInputFormat,
                dateOutputFormat = properties[PROP_DATE_OUTPUT_FORMAT] ?: defaults.dateOutputFormat,
            )
        }
    }
}

internal data class OdmRecord(
    val subjectKey: String,
    val studyEventOid: String,
    val studyEventRepeatKey: String?,
    val formOid: String,
    val itemGroupOid: String,
    val igRepeatKey: String,
    val items: List<OdmItem>,
)

internal data class OdmItem(
    val itemOid: String,
    val value: String,
)
