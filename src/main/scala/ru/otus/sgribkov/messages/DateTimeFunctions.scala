package ru.otus.sgribkov.messages

import java.time.{LocalDate, LocalDateTime, ZoneId, ZoneOffset}
import java.time.format.DateTimeFormatter
import scala.collection.mutable.ArrayBuffer

trait DateTimeFunctions {

  def dateToUnixTimestamp(date: String, dateFormat: String = "yyyy-MM-dd") = {

    val dateFormatter = DateTimeFormatter.ofPattern(dateFormat)
    val zoneId = ZoneId.systemDefault()

    LocalDate
      .parse(date, dateFormatter)
      .atTime(0, 0, 0)
      .atZone(zoneId)
      .toEpochSecond()

  }

}
