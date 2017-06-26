package enron

import enron.email.{Email, PSTParser}
import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner
import org.specs2.mutable._

@RunWith(classOf[JUnitRunner])
class PSTParserTest extends Specification {

  val pSTReader = new PSTParser

  "PSTReader" should {
    "Read email" in {
      val emails = pSTReader.processFileURI("./src/test/resources/dist-list.pst")
      emails.get.size mustEqual 4
      emails.get(0) mustEqual Email(None, Some("Test appointment"), Some("This is a complete test"), Some("Unknown"), Nil)
    }
    "Fail when corrupted" in {
      val emails = pSTReader.processFileURI("./src/test/resources/corrupted.pst")
      emails.isFailure mustEqual true
    }
  }

}
