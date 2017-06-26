package enron.email

import java.io.InputStream
import javax.mail.Message.RecipientType
import javax.mail.Message.RecipientType._
import javax.mail.Session
import javax.mail.internet.MimeMessage

import org.apache.commons.lang3.StringUtils.isNotBlank

import scala.util.{Failure, Success, Try}

class EMLParser extends EmailParser {

  private val messageIdPattern = "<(.*)>".r

  override def process(source: InputStream): Try[List[Email]] = {
    try {
      val props = System.getProperties
      props.put("mail.host", "smtp.dummydomain.com")
      props.put("mail.transport.protocol", "smtp")
      props.put("mail.mime.address.strict", "false")

      val mailSession = Session.getDefaultInstance(props, null)
      val message = new MimeMessage(mailSession, source)
      val recipients = if (message.getAllRecipients != null) {
        List(TO,CC,BCC).flatMap(recipientOfType(message, _))
      }
      else Nil

      val from = if (message.getFrom != null && message.getFrom.toList.nonEmpty) Some(message.getFrom.toList.head.toString) else None
      val content = if (isNotBlank(message.getContent.toString)) Some(message.getContent.toString) else None
      val subject = if (isNotBlank(message.getSubject)) Some(message.getSubject) else None

      val messageID = message.getMessageID match {
        case messageIdPattern(x) => Some(x)
        case _ => None
      }

      if (messageID.isEmpty && from.isEmpty && content.isEmpty && subject.isEmpty && recipients.isEmpty) {
        Failure(new IllegalArgumentException("Empty file"))
      } else {
        Success(List(Email(messageID, subject, content, from, recipients)))
      }
    } catch {
      case e: Exception => e.printStackTrace(); Failure(e)
    } finally {
      source.close()
    }
  }

  private def recipientOfType(message: MimeMessage, recipientType: RecipientType) = {
    val recipients = message.getRecipients(recipientType)
    if (recipients != null) recipients.toList.map(r => Recipient(r.toString, recipientType.toString)) else Nil
  }
}
