package enron.processors

import enron.{AverageLengthProcessor, Context}
import org.junit.runner.RunWith
import org.specs2.mutable._
import org.specs2.runner.JUnitRunner


@RunWith(classOf[JUnitRunner])
class AverageLengthProcessorTest extends Specification {


  "AverageLengthProcessor" should {
    "Return top N recipient" in new Context {
      val (inputPath, outputPath) = prepareInOutEmails("AverageLengthProcessor")(createEmailsDF)
      val processor = AverageLengthProcessor(inputPath, outputPath)
      
      processor.run(testSpark)

      val average = testSpark.read.parquet(outputPath).collectAsList()

      average.size() mustEqual 1
      average.get(0)(0) mustEqual 2.0
    }
  }
}

