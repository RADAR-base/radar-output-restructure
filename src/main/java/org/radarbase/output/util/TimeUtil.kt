package org.radarbase.output.util

import com.fasterxml.jackson.databind.JsonNode
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.radarbase.output.path.RecordPathFactory.Companion.getFieldOrNull
import java.math.RoundingMode
import java.time.*
import java.time.format.DateTimeParseException
import java.time.temporal.Temporal

object TimeUtil {
    private val NANO_MULTIPLIER = 1_000_000_000.toBigDecimal()

    /**
     * Get the date contained in given records
     * @param key key field of the record
     * @param value value field of the record
     * @return date contained in the values of either record, or `null` if not found or
     * it cannot be parsed.
     */
    fun getDate(
        key: GenericRecord?,
        value: GenericRecord?,
    ): Instant? {
        value?.timeOrNull("time")
            ?.let { return it }

        key?.run {
            timeOrNull("timeStart")
                ?.let { return it }

            getFieldOrNull("start")
                ?.takeIf { it.hasType(Schema.Type.LONG) }
                ?.let { get(it.pos()) as? Long }
                ?.let { return Instant.ofEpochMilli(it) }
        }

        value?.run {
            dateTimeOrNull("dateTime")
                ?.let { return it }

            dateOrNull("date")
                ?.let { return it }

            timeOrNull("timeReceived")
                ?.let { return it }
            timeOrNull("timeCompleted")
                ?.let { return it }
        }

        return null
    }

    fun getDate(key: JsonNode?, value: JsonNode?): Double? {
        value?.getOrNull("time")
            ?.takeIf { it.isNumber }
            ?.let { return it.asDouble() }

        key?.run {
            getOrNull("timeStart")
                ?.takeIf { it.isNumber }
                ?.let { return it.asDouble() }

            getOrNull("start")
                ?.takeIf { it.isNumber }
                ?.let { return it.asLong() / 1000.0 }
        }

        value?.run {
            getOrNull("dateTime")
                ?.takeIf { it.isTextual }
                ?.let { node -> return node.asText().parseDateTime()?.toDouble() }

            getOrNull("date")
                ?.takeIf { it.isTextual }
                ?.let { node -> return node.asText().parseDate()?.toDouble() }

            getOrNull("timeReceived")
                ?.takeIf { it.isNumber }
                ?.let { return it.asDouble() }

            getOrNull("timeCompleted")
                ?.takeIf { it.isNumber }
                ?.let { return it.asDouble() }
        }

        return null
    }

    private fun GenericRecord.timeOrNull(fieldName: String): Instant? = getFieldOrNull(fieldName)
        ?.takeIf { it.hasType(Schema.Type.DOUBLE) }
        ?.let { (get(it.pos()) as? Double) }
        ?.toInstant()

    /**
     * Parse the dateTime field of a record, if present.
     *
     * @param fieldName field that contains the date time
     * @return `Instant` representing the dateTime or `null` if the field cannot be
     * found or parsed.
     */
    private fun GenericRecord.dateTimeOrNull(fieldName: String): Instant? =
        getFieldOrNull(fieldName)
            ?.takeIf { it.hasType(Schema.Type.STRING) }
            ?.let { get(it.pos()) }
            ?.toString()
            ?.parseDateTime()

    /**
     * Parse the date field of a record, if present.
     *
     * @param fieldName field that contains the date
     * @return `Instant` representing the start of given date or `null` if the field
     * cannot be found or parsed.
     */
    private fun GenericRecord.dateOrNull(fieldName: String): Instant? = getFieldOrNull(fieldName)
        ?.takeIf { it.hasType(Schema.Type.STRING) }
        ?.let { get(it.pos()) }
        ?.toString()
        ?.parseDate()

    private fun Double.toInstant(): Instant {
        val time = toBigDecimal()
        val seconds = time.toLong()
        val nanoseconds = ((time - seconds.toBigDecimal()) * NANO_MULTIPLIER).toLong()
        return Instant.ofEpochSecond(seconds, nanoseconds)
    }

    fun String.parseTime(): Double? = toDoubleOrNull()

    fun String.parseDate(): Instant? = try {
        LocalDate.parse(this)
            .atStartOfDay(ZoneOffset.UTC)
            .toInstant()
    } catch (ex: DateTimeParseException) {
        null
    }

    fun String.parseDateTime(): Instant? = try {
        if (this[lastIndex] == 'Z') {
            Instant.parse(this)
        } else {
            LocalDateTime.parse(this).toInstant(ZoneOffset.UTC)
        }
    } catch (ex: DateTimeParseException) {
        null
    }

    fun Instant.toDouble() = (
        epochSecond.toBigDecimal() +
            (nano.toBigDecimal().divide(NANO_MULTIPLIER, 9, RoundingMode.HALF_UP))
        ).toDouble()

    private fun JsonNode.getOrNull(fieldName: String): JsonNode? = fields().asSequence()
        .find { (name, _) -> name.equals(fieldName, ignoreCase = true) }
        ?.value

    private fun Schema.Field.hasType(type: Schema.Type): Boolean {
        val s = schema()
        return s.type == type ||
            (s.type == Schema.Type.UNION && s.types.any { it.type == type })
    }

    fun Temporal.durationSince(): Duration = Duration.between(this, Instant.now())
}
