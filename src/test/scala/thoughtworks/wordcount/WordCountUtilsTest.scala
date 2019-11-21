package thoughtworks.wordcount

import org.apache.spark.sql.{DataFrame, Dataset}
import WordCountUtils._
import thoughtworks.DefaultFeatureSpecWithSpark


class WordCountUtilsTest extends DefaultFeatureSpecWithSpark {
  feature("Split Words") {
    scenario("test splitting a dataset of words by spaces") {
      import spark.implicits._

      val inputText = Seq("Hello Awesome World").toDS()
      val splittedWords = inputText splitWords spark
      val expectedWords = Array("hello", "awesome", "world")

      splittedWords.collect() should contain theSameElementsAs expectedWords
    }

    scenario("test splitting a dataset of words by period") {
      import spark.implicits._

      val inputText = Seq("Hello.Awesome.World").toDS()
      val splittedWords = inputText splitWords spark
      val expectedWords = Array("hello", "awesome", "world")

      splittedWords.collect() should contain theSameElementsAs expectedWords
    }

    scenario("test splitting a dataset of words by comma") {
      import spark.implicits._

      val inputText = Seq("Hello,Awesome,World").toDS()
      val splittedWords = inputText splitWords spark
      val expectedWords = Array("hello", "awesome", "world")

      splittedWords.collect() should contain theSameElementsAs expectedWords
    }

    scenario("test splitting a dataset of words by hypen") {
      import spark.implicits._

      val inputText = Seq("Hello-Awesome-World").toDS()
      val splittedWords = inputText splitWords spark
      val expectedWords = Array("hello", "awesome", "world")

      splittedWords.collect() should contain theSameElementsAs expectedWords
    }

    scenario("test splitting a dataset of words by semi-colon") {
      import spark.implicits._

      val inputText = Seq("Hello;Awesome;World").toDS()
      val splittedWords = inputText splitWords spark
      val expectedWords = Array("hello", "awesome", "world")

      splittedWords.collect() should contain theSameElementsAs expectedWords
    }

    scenario("test case insensitivity") {
      import spark.implicits._

      val splittedWords = Seq("Hello", "Awesome", "AwesoME", "world", "World").toDS()
      val wordCount = splittedWords splitWords spark
      val expectedWords = Array("hello", "awesome", "awesome", "world", "world")

      wordCount.collect() should contain theSameElementsAs expectedWords
    }
  }

  feature("Count Words") {
    scenario("basic test case") {

      import spark.implicits._

      val splittedWords = Seq("Hello", "Awesome", "World", "World").toDS()
      val wordCount = splittedWords countByWord spark
      val expectedWordsCount = Array("Hello,1", "Awesome,1", "World,2")

      wordCount.collect() should contain theSameElementsAs expectedWordsCount
    }

    scenario("should not aggregate dissimilar words") {
      import spark.implicits._

      val splittedWords = Seq("Hello", "Awesome", "World", "World").toDS()
      val wordCount = splittedWords countByWord spark
      val expectedWordsCount = Array("Hello,1", "Awesome,1", "World,2")

      wordCount.collect() should contain theSameElementsAs expectedWordsCount
    }

  }

  feature("Sort Words") {
    scenario("test ordering words") {
      import spark.implicits._

      val splittedWords = Seq("Hello", "Awesome", "World", "World").toDS()
      val wordCount = splittedWords countByWord spark
      val expectedWordsCount = Array("Awesome,1", "Hello,1", "World,2")

      wordCount.collect() should be(expectedWordsCount)
    }
  }

}
