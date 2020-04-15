package org.radarbase.output

import org.junit.jupiter.api.Test
import java.time.LocalDate
import java.time.format.DateTimeFormatter

class DateTest {
    @Test
    fun dateTest() {
        val dateString = "2017-01-01"
        val date = LocalDate.from(DateTimeFormatter.ofPattern("yyyy-MM-dd").parse(dateString))
        println(date)
        println(date.toEpochDay())
    }
}
