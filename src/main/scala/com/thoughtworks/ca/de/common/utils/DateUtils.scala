package com.thoughtworks.ca.de.common.utils

import java.time.format.DateTimeFormatter
import java.time.{Clock, LocalDate, OffsetDateTime, ZoneId}

object DateUtils {
  private val twDateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd").withZone(OffsetDateTime.now().getOffset)

  def parseISO2TWFormat(isoDate: String): String = {
    LocalDate.parse(isoDate).format(twDateTimeFormatter)
  }

  def date2TWFormat(implicit clock: Clock): String = {
    twDateTimeFormatter.format(clock.instant())
  }
}
