package enron

import java.util.Properties

import etl.spark.SparkProcessor
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.slf4j.{Logger, LoggerFactory}


class TopRecipientsProcessor(config: TopRecipientsProcessorConfig) extends SparkProcessor {

  val log: Logger = LoggerFactory.getLogger(classOf[TopRecipientsProcessor])

  def run(sparkSession: SparkSession): Unit = {
    import sparkSession.implicits._
    log.info(">>>>>>>> Started JOB")
    val emailDF = sparkSession.read.parquet(config.processedPath)


    val recipientScore = udf { (recipientType: String) =>
      recipientType match {
        case "To" => 1
        case "Cc" | "Bcc" => 0.5
        case _ => 0
      }
    }

    val recipientsDf = emailDF.select(explode($"recipients").alias("r"))
                              .select($"r.emailAddress".alias("recipient"), recipientScore($"r.recipientType").alias("score"))
    val topNRecipients = recipientsDf.groupBy($"recipient").agg(sum("score").alias("totalScore")).orderBy($"totalScore".desc).limit(config.topN)
    topNRecipients.write.parquet(config.outputPath)
    log.info(">>>>>>>> Finshed JOB")
  }

}

object TopRecipientsProcessor {
  def apply(prop: Properties): TopRecipientsProcessor = new TopRecipientsProcessor(TopRecipientsProcessorConfig(prop))
  def apply(processedPath: String, outputPath: String, topN: Int): TopRecipientsProcessor = new TopRecipientsProcessor(TopRecipientsProcessorConfig(processedPath, outputPath, topN))

}

case class TopRecipientsProcessorConfig(processedPath: String, outputPath: String, topN: Int)

object TopRecipientsProcessorConfig {
  def apply(prop: Properties): TopRecipientsProcessorConfig = {
    new TopRecipientsProcessorConfig(prop.getProperty("processedPath"), prop.getProperty("outputPath"), prop.getProperty("topN").toInt)
  }

}

