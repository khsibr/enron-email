package enron.email

import java.io.{File, FileOutputStream, InputStream}
import javax.mail.Message.RecipientType

import com.pff._
import org.apache.commons.io.IOUtils
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.StringUtils.isNotBlank

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

/**
  * This class is responsible parsing PST files.
  *
  */
class PSTParser() extends EmailParser {


  override def process(source: InputStream): Try[List[Email]] = {
    val tmpFile = File.createTempFile("pst_parser_tmp", ".pst")
    try {
      val fos = new FileOutputStream(tmpFile)
      IOUtils.copy(source,fos);
      fos.close()
      val pstFile = new PSTFile(tmpFile)
      val success = Success(processFolder(pstFile.getRootFolder()))
      pstFile.close()
      success
    } catch {
      case err: Exception => Failure(err)
    } finally {
      tmpFile.delete()
      source.close()
    }
  }

  def processFolder(folder: PSTFolder): List[Email] = {
    val emails = new ListBuffer[Email]
    processFolderRecursive(folder, emails)
    emails.toList
  }

  def processFolderRecursive(folder: PSTFolder, current: ListBuffer[Email]) {

    // go through the folders...
    if (folder.hasSubfolders()) {
      val childFolders = folder.getSubFolders().asScala
      for (childFolder <- childFolders) {
        processFolderRecursive(childFolder, current)
      }
    }

    // and now the emails for this folder
    if (folder.getContentCount() > 0) {
      var email = folder.getNextChild().asInstanceOf[PSTMessage]
      while (email != null) {
        val body = if (email.getBody() != null) Some(email.getBody().trim) else None
        val messageId = if (isNotBlank(email.getInternetMessageId)) Some(email.getInternetMessageId) else None
        current.append(Email(messageId, Some(email.getSubject()), body, Some(email.getSenderEmailAddress), recipients(email)))
        email = folder.getNextChild().asInstanceOf[PSTMessage]
      }
    }
  }

  private def recipients(email: PSTMessage) = {
    (for (i <- 0 until email.getNumberOfRecipients)
      yield Recipient(email.getRecipient(i).getEmailAddress, recipientType(email.getRecipient(i).getRecipientType))
      ).toList
  }

  private def recipientType(intType: Int): String = {
    intType match {
      case PSTMessage.RECIPIENT_TYPE_CC => RecipientType.CC.toString
      case PSTMessage.RECIPIENT_TYPE_TO => RecipientType.TO.toString
      case _ => ""
    }
  }

}

