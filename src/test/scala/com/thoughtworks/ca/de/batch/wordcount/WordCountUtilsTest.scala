package com.thoughtworks.ca.de.batch.wordcount

import com.thoughtworks.ca.de.DefaultFeatureSpecWithSpark
import com.thoughtworks.ca.de.batch.wordcount.WordCountUtils._
import org.apache.spark.sql.Dataset


class WordCountUtilsTest extends DefaultFeatureSpecWithSpark {
  feature("Split Words") {
    ignore("test splitting a dataset of words by spaces") {}

    ignore("test splitting a dataset of words by period") {}

    ignore("test splitting a dataset of words by comma") {}

    ignore("test splitting a dataset of words by hypen") {}

    ignore("test splitting a dataset of words by semi-colon") {}
  }

  feature("Count Words") {
    ignore("basic test case") {}

    ignore("should not aggregate dissimilar words") {}

    ignore("test case insensitivity") {}
  }

  feature("Sort Words") {
    ignore("test ordering words") {}
  }

}
