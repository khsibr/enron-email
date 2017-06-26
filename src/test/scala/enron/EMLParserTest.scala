package enron

import enron.email.{EMLParser, Email, Recipient}
import org.junit.runner.RunWith
import org.specs2.mutable._
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class EMLParserTest extends Specification {

  val pSTReader = new EMLParser

  "EMLParser" should {
    "Read email" in {
      val emails = pSTReader.processFileURI("./src/test/resources/valid.eml")
      emails.get.size mustEqual 1
      emails.get(0) mustEqual Email(Some("00000000270CEF63F6565D4B902E390D32278E5144FC2000@PMZL01"), Some("ken lay tour"), Some("Just so we're all on the same page."),
        Some("Janel Guerrero"), List(Recipient("Rosalee Fleming","To"), Recipient("Maureen McVicker","To"), Recipient("Jeff Dasovich","To"),
          Recipient("Richard Shapiro","Cc"), Recipient("Paul Kaufman","Cc")))
    }
    "Fail when corrupted" in {
      val emails = pSTReader.processFileURI("./src/test/resources/corrupted.eml")
      emails.isFailure mustEqual true
    }
  }

}
