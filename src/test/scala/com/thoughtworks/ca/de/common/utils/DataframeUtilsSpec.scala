package com.thoughtworks.ca.de.common.utils

import com.thoughtworks.ca.de.DefaultFeatureSpecWithSpark

class DataframeUtilsSpec extends DefaultFeatureSpecWithSpark {
  info("As a user of Dataframe Utilities")
  info("I want to be able to format column headers in data frame")
  info("So it can be compatible with storage formats like Parquet and Avro")

  feature("Format column headers in data frame") {
    scenario("Data frame is passed to utility") {

      Given("Data frame that contains column headers with white spaces")

      import spark.implicits._
      val testDF = Seq(
        ("20180301", 9089),
        ("20180302", 6787),
        ("20180302", 10987)
      ).toDF("financial day", "total revenue")

      When("Dataframe utils is used")

      val resultDF = DataframeUtils.formatColumnHeaders(testDF)

      Then("Column headers are cleaned of white spaces")

      val expectedColumns = Array("financial_day", "total_revenue")
      resultDF.columns should be(expectedColumns)
    }
  }
}
