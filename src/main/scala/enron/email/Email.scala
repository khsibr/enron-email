package enron.email

import java.io.{File, FileInputStream, InputStream}
import java.net.URI
import java.nio.file.Paths

import org.apache.commons.io.FilenameUtils.getExtension

import scala.util.Try
case class EnronEmailExtract(fileName: String, emails: List[Email])

case class Email(messageId: Option[String], subject: Option[String], body: Option[String], from: Option[String], recipients: List[Recipient])

case class Recipient(emailAddress: String, recipientType: String)

trait EmailParser {
  def processFileURI(fileURI: String): Try[List[Email]] = {
    val uri = new URI(fileURI)
    val file = uri.getScheme match {
      case "file" => Paths.get(uri).toFile()
      case null => new File(fileURI)
    }
    process(new FileInputStream(file))
  }

  def process(source: InputStream): Try[List[Email]]
}

object EmailParser {

  def apply(file: String): Option[EmailParser] = {
    getExtension(file) match {
      case "pst" => Some(new PSTParser)
      case "eml" => Some(new EMLParser)
      case _ => None
    }
  }

}

