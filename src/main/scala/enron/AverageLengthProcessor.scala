package enron

import java.util.Properties

import etl.spark.SparkProcessor
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}
import org.apache.spark.sql.functions._

/**
  * This processor is responsible for computing the average length, in words, of the emails.
  *
  */
class AverageLengthProcessor(config: AverageLengthProcessorConfig) extends SparkProcessor {

  val log: Logger = LoggerFactory.getLogger(classOf[AverageLengthProcessor])

  def run(sparkSession: SparkSession): Unit = {
    import sparkSession.implicits._
    log.info(">>>>>>>> Started JOB")

    val tokenize = udf { (text: String) => text.split("\\s+") }
    val emailDF = sparkSession.read.parquet(config.processedPath)
    val averageEmailLengthDF = emailDF.select(avg(size(tokenize($"body"))).alias("avg_body_length"))
    averageEmailLengthDF.write.parquet(config.outputPath)

    val result = averageEmailLengthDF.collect()(0).getDouble(0)
    log.info(s"**************** averageEmailLength: $result **********************")
    log.info(">>>>>>>> Finshed JOB")
  }

}

object AverageLengthProcessor {
  def apply(prop: Properties): AverageLengthProcessor = new AverageLengthProcessor(AverageLengthProcessorConfig(prop))
  def apply(processedPath: String, outputPath: String): AverageLengthProcessor = new AverageLengthProcessor(AverageLengthProcessorConfig(processedPath, outputPath))
}

case class AverageLengthProcessorConfig(processedPath: String, outputPath: String)

object AverageLengthProcessorConfig {
  def apply(prop: Properties): AverageLengthProcessorConfig = {
    new AverageLengthProcessorConfig(prop.getProperty("processedPath"), prop.getProperty("outputPath"))
  }

}

