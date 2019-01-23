package com.thoughtworks.ca.de.batch.ingest_to_data_lake

import com.thoughtworks.ca.de.common.utils.DataframeUtils
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.sql.SparkSession

object DailyDriver {
  val log: Logger = LogManager.getRootLogger

  def main(args: Array[String]) {
    log.setLevel(Level.INFO)
    val spark =
      SparkSession.builder.appName("Skinny Pipeline: Ingest").getOrCreate()
    log.info("Application Initialized: " + spark.sparkContext.appName)

    //Parse argument/s
    if (args.length < 2) {
      spark.stop()
      log.warn("Input source and output path are required")
      System.exit(1)
    }
    val inputSource = args(0)
    val outputPath = args(1)

    run(spark, inputSource, outputPath)

    log.info("Application Done: " + spark.sparkContext.appName)
    spark.stop()
  }

  def run(spark: SparkSession, inputSource: String, outputPath: String): Unit = {
    val inputDataFrame = spark.read
      .format("org.apache.spark.csv")
      .option("header", value = true)
      .csv(inputSource)
    DataframeUtils.formatColumnHeaders(inputDataFrame)
      .write
      .parquet(outputPath)
  }
}
