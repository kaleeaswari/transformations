package thoughtworks.wordcount

import org.apache.spark.sql.{Dataset, SparkSession, Row}
import org.apache.spark.sql.functions.{concat, lit}

object WordCountUtils {

  implicit class StringDataset(val dataSet: Dataset[String]) {
    def splitWords(spark: SparkSession) = {
      import spark.implicits._
      dataSet.map(line => line.toLowerCase().replaceAll("\"", "").trim().split(" |,|;|\\.|-"))
        .flatMap(word => word)
        .filter(_.length >= 1 )

    }

    def countByWord(spark: SparkSession) = {
      import spark.implicits._
      val output = dataSet.groupBy($"value").count().select(concat($"value" , lit(","),  $"count") as "wordCount").orderBy($"value")
      output.as[String]
    }
  }

}
