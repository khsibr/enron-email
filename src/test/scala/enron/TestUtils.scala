package enron

import java.io.File
import java.nio.file.Files

import enron.TestUtils.mockEmails
import enron.email.{Email, Recipient}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.specs2.mutable.BeforeAfter


object TestUtils {
  def mockEmails: List[Email] = {
    List(
      Email(Some("email1"), Some("subject1"), Some("body1 test"), Some("from1"), List(Recipient("to1","To"), Recipient("to2","Cc"))),
      Email(Some("email2"), Some("subject2"), Some("body2 test test"), Some("from2"), List(Recipient("to3","To"), Recipient("to1","Cc"))),
      Email(Some("email3"), Some("subject3"), Some("body3"), Some("from3"), List(Recipient("to3","To"), Recipient("to4","Cc")))
    )
  }


}
trait Context extends BeforeAfter {
  var testSpark:SparkSession = _
  var tempInDir: File = _
  var tempOutDir: File = _

  def before: Any = {
    testSpark = SparkSession.builder.
      master("local")
      .appName("spark test")
      .getOrCreate()

    tempInDir = Files.createTempDirectory("enron_spark_test_input").toFile
    tempOutDir = Files.createTempDirectory("enron_spark_test_output").toFile
  }

  def after: Any = {
    testSpark.stop()
    FileUtils.forceDelete(tempInDir)
    FileUtils.forceDelete(tempOutDir)
  }

  def createEmailsDF(inputPath: String): Unit = {
    val spark = testSpark
    import spark.implicits._

    val emails = mockEmails
    val emailsDF = testSpark.createDataset[Email](emails)
    emailsDF.write.parquet(inputPath)
  }

  def copyFiles(filePath: String)(inputPath: String): Unit = {
    FileUtils.copyFileToDirectory(new File(filePath), new File(inputPath))
  }

  def prepareInOutEmails(prefix: String)(post: String => Unit): (String, String) = {
    val outputPath = tempOutDir.getAbsolutePath + "/" + prefix + "Output"
    val inputPath = tempInDir.getAbsolutePath + "/" + prefix + "Input"

    post(inputPath)

    (inputPath, outputPath)
  }
}
