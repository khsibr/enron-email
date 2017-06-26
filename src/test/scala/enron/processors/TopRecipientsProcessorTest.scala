package enron.processors

import enron.{Context, TopRecipientsProcessor}
import org.junit.runner.RunWith
import org.specs2.mutable._
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TopRecipientsProcessorTest extends Specification {


  "TopRecipientsProcessor" should {
    "Return top N recipients" in new Context {
      val (inputPath, outputPath) = prepareInOutEmails("TopRecipientsProcessor")(createEmailsDF)
      val processor = TopRecipientsProcessor(inputPath, outputPath, 2)

      processor.run(testSpark)

      val recipients = testSpark.read.parquet(outputPath).collectAsList()

      recipients.size() mustEqual 2
      recipients.get(0)(0) mustEqual "to3"
      recipients.get(0)(1) mustEqual 2.0
      recipients.get(1)(0) mustEqual "to1"
      recipients.get(1)(1) mustEqual 1.5
    }
  }
}

