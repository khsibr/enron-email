package enron.processors

import enron.{Context, EnronEmailProcessor}
import org.apache.spark.sql.Row
import org.junit.runner.RunWith
import org.specs2.mutable._
import org.specs2.runner.JUnitRunner

import scala.collection.mutable


@RunWith(classOf[JUnitRunner])
class EnronEmailProcessorTest extends Specification {


  "EnronEmailProcessor" should {
    "Parse EML files in zip" in new Context {
      val (inputPath, outputPath) = prepareInOutEmails("EnronEmailProcessor")(copyFiles("./src/test/resources/emails.zip"))
      val processor = EnronEmailProcessor(inputPath, outputPath, "eml")
      
      processor.run(testSpark)

      val emails = testSpark.read.parquet(outputPath).collectAsList()

      emails.size() mustEqual 1
      val row = emails.get(0)
      row(0) mustEqual "00000000270CEF63F6565D4B902E390D32278E5144FC2000@PMZL01"
      row(1) mustEqual "ken lay tour"
      row(2) mustEqual "Just so we're all on the same page."
      row(3) mustEqual "Janel Guerrero"
      val recipients = row(4).asInstanceOf[mutable.WrappedArray[Row]]
      recipients(0)(0) mustEqual "Rosalee Fleming"
      recipients(0)(1) mustEqual "To"
      recipients(1)(0) mustEqual "Maureen McVicker"
      recipients(1)(1) mustEqual "To"
      recipients(2)(0) mustEqual "Jeff Dasovich"
      recipients(2)(1) mustEqual "To"
      recipients(3)(0) mustEqual "Richard Shapiro"
      recipients(3)(1) mustEqual "Cc"
      recipients(4)(0) mustEqual "Paul Kaufman"
      recipients(4)(1) mustEqual "Cc"
    }
  }
}

