package com.thoughtworks.ca.de.common.utils

import org.apache.spark.sql.DataFrame

object DataframeUtils {
  def formatColumnHeaders(dataFrame: DataFrame): DataFrame = {
    var retDf = dataFrame
    for (column <- retDf.columns) {
      retDf = retDf.withColumnRenamed(column, column.replaceAll("\\s", "_"))
    }
    retDf.printSchema()
    retDf
  }
}
