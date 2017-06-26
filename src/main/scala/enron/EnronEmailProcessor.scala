package enron

import java.io.ByteArrayInputStream
import java.util.Properties

import com.cotdp.hadoop.ZipFileInputFormat
import enron.email.{Email, EmailParser}
import etl.spark.SparkProcessor
import org.apache.commons.io.FilenameUtils.getExtension
import org.apache.hadoop.io.{BytesWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success}

/**
  * This processor is responsible for processing the Enron Emails Dataset in order to build a clean parquet view.
  *
  */
class EnronEmailProcessor(config: EnronEmailProcessorConfig) extends SparkProcessor {

  val log: Logger = LoggerFactory.getLogger(classOf[EnronEmailProcessor])

  def run(sparkSession: SparkSession): Unit = {
    import sparkSession.implicits._
    val sc = sparkSession.sparkContext
    log.info(">>>>>>>> Started JOB")

    val zipFileRDD = sc.newAPIHadoopFile(
      config.emailsPath,
      classOf[ZipFileInputFormat],
      classOf[Text],
      classOf[BytesWritable],
      Job.getInstance.getConfiguration)

    val emailFormat = config.emailFormat

    // Parse Emails
    // EML format preferred because PST library doesn't support input stream as input
    // and because EML files are typically smaller (~1MB) vs PST files can be very large (~1GB)
    val emailsRdd = zipFileRDD.collect {
      case (s, w) if getExtension(s.toString) == emailFormat =>
        val fileName = s.toString
        val triedEmails = EmailParser(fileName).map { p =>
          p.process(new ByteArrayInputStream(w.copyBytes()))
        }
        triedEmails.collect {
          case Success(emails) => (emails, 0)
          case Failure(e) => println("Error parsing email", e); (Nil, 1)
        }.getOrElse((Nil, 0))
    }

    // Compute parse stats
    val stats = emailsRdd.aggregate((0, 0))(
      (accum, res2) => (accum, res2) match {
        case ((totalSuccessCount, totalErrorCount), (emails, errorCount)) => (totalSuccessCount + emails.size, totalErrorCount + errorCount)
      },
      (accum1, accum2) => (accum1, accum2) match {
        case ((successCount1, errorCount1), (successCount2, errorCount2)) => (successCount1 + successCount2, errorCount1 + errorCount2)
      }
    )
    log.info(s"**************** Stats: Success ${stats._1}, Failures ${stats._2} **********************")

    emailsRdd.flatMap(_._1).toDS().write.parquet(config.outputPath)

    log.info(">>>>>>>> Finshed JOB")

  }

}

object EnronEmailProcessor {
  def apply(prop: Properties): EnronEmailProcessor = new EnronEmailProcessor(EnronEmailProcessorConfig(prop))

  def apply(emailsPath: String, outputPath: String, emailFormat: String): EnronEmailProcessor = new EnronEmailProcessor(EnronEmailProcessorConfig(emailsPath, outputPath, emailFormat))
}

case class EnronEmailProcessorConfig(emailsPath: String, outputPath: String, emailFormat: String)

object EnronEmailProcessorConfig {
  def apply(prop: Properties): EnronEmailProcessorConfig = {
    new EnronEmailProcessorConfig(prop.getProperty("emailsPath"), prop.getProperty("outputPath"), prop.getProperty("emailFormat"))
  }

}

case class EnronEmailExtract(fileName: String, emails: List[Email])
