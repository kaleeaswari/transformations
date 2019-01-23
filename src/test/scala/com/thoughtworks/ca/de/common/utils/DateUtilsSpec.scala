package com.thoughtworks.ca.de.common.utils

import java.time.{Clock, OffsetDateTime}

import org.scalatest._

class DateUtilsSpec extends FeatureSpec with GivenWhenThen {
  info("As a user of Date Utilities")
  info("I want to be able to parse date string to ISO format")
  info("So it can be used to create directory structure by dates")
  info("And enable daily partitions, auditing and versioning")

  feature("Date format conversion") {
    scenario("User passes date as parameter to driver program in yyyy-MM-dd format") {

      Given("Date 2018-03-26 is provided for conversion")
      val argumentDate = "2018-03-26"

      When("Conversion is performed")
      val returnStr = DateUtils.parseISO2TWFormat(argumentDate)

      Then("Date is converted to 20180326")
      assert(returnStr.equals("20180326"))
    }

    scenario("User does not provide date parameter") {
      val offset = OffsetDateTime.now().getOffset
      implicit val clock: Clock = Clock.fixed(
        OffsetDateTime.parse(s"2018-03-19T00:00:00${offset.getId}").toInstant,
        offset)
      Given("No arguments provided")

      When("Conversion is performed")
      val returnStr = DateUtils.date2TWFormat

      Then("Today's Date is converted to 20180319")
      assert(returnStr.equals("20180319"))
    }
  }
}
