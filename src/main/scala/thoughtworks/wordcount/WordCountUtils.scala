package thoughtworks.wordcount

import org.apache.spark.sql.{Dataset, SparkSession, Row}
import org.apache.spark.sql.functions.{concat, lit}

object WordCountUtils {

  implicit class StringDataset(val dataSet: Dataset[String]) {
    def splitWords(spark: SparkSession) = {
      import spark.implicits._
      dataSet.map(line => line.toLowerCase().replaceAll("\"", "")
        .split(" |,|;|\\.|-")).flatMap(word => if (word.length >=1) word else None)
    }

    def countByWord(spark: SparkSession) = {
      import spark.implicits._
      val output = dataSet.groupBy($"value").count().select(concat($"value" , lit(","),  $"count") as "wordCount").orderBy($"value")
      output.as[String]
    }
  }

}
